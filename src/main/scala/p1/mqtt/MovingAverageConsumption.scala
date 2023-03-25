package p1.mqtt

import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt.MqttConnectionSettings
import akka.stream.alpakka.mqtt.MqttMessage
import akka.stream.alpakka.mqtt.MqttQoS
import akka.stream.alpakka.mqtt.MqttSubscriptions
import akka.stream.alpakka.mqtt.scaladsl.MqttSink
import akka.stream.alpakka.mqtt.scaladsl.MqttSource
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import net.liftweb.json.Serialization.write
import net.liftweb.json._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import akka.stream.Attributes

object MovingAverageConsumption extends App with LazyLogging {

  implicit val system = ActorSystem("moving-average-consumption")
  implicit val executionContext = system.dispatcher

  val config = ConfigFactory.load()
  val platform = config.getString("platform")

  logger.info(f"running on platform: ${platform}")

  val mqttUrl = config.getString("mqtt.url")
  val mqttUser = config.getString("mqtt.user")
  val mqttPassword = config.getString("mqtt.password")

  val mqttSourceTopic =
    f"${config.getString("mqtt.average.topics.source")}"
  val mqttTargetTopic =
    f"${config.getString("mqtt.average.topics.target")}-${platform}"
  val consumerId =
    f"${config.getString("mqtt.average.consumer-id")}-${platform}"
  val producerId =
    f"${config.getString("mqtt.average.producer-id")}-${platform}"

  val consumerSettings =
    MqttConnectionSettings(mqttUrl, consumerId, new MemoryPersistence)
      .withAutomaticReconnect(true)
      .withAuth(mqttUser, mqttPassword)

  val producerSettings =
    MqttConnectionSettings(mqttUrl, producerId, new MemoryPersistence)
      .withAutomaticReconnect(true)
      .withAuth(mqttUser, mqttPassword)

  val mqttSource = MqttSource.atMostOnce(
    consumerSettings,
    MqttSubscriptions(Map(mqttSourceTopic -> MqttQoS.AtLeastOnce)),
    bufferSize = 20
  )

  val mqttSink = MqttSink(producerSettings, MqttQoS.AtLeastOnce)

  val formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME

  mqttSource
    .map(msg => Consumption(parse(msg.payload.utf8String)))
    .async
    .statefulMapConcat { () =>
      var values = new ListBuffer[Double]()

      { element =>
        values += element.currentConsumption
        val n = values.length
        val movingAverage = (values.sum / n) * 1000 // kW -> W
        val dateTime = ZonedDateTime.parse(element.dateTime, formatter)
        if ((dateTime.getMinute() % 15 == 0) && (dateTime.getSecond() == 0)) {
          values = new ListBuffer[Double]()
        }
        AverageConsumption(
          movingAverage,
          element.currentAverageDemand,
          n,
          element.dateTime
        ) :: Nil
      }
    }
    .log(name = mqttTargetTopic)
    .withAttributes(
      Attributes.logLevels(
        onElement = Attributes.LogLevels.Info,
      )
    )
    .map(energy => MqttMessage(mqttTargetTopic, energy.toBytes))
    .runWith(mqttSink)
}

case class Consumption(
    currentConsumption: Double,
    currentAverageDemand: Double,
    dateTime: String
)

case object Consumption {
  implicit val formats = DefaultFormats
  def apply(json: JValue): Consumption = {
    val consumption =
      (json \ "CURRENT_ELECTRICITY_USAGE" \ "value").extract[Double]
    val currentAverageDemand =
      (json \ "BELGIUM_CURRENT_AVERAGE_DEMAND" \ "value").extract[Double]
    val dateString = (json \ "P1_MESSAGE_TIMESTAMP" \ "value").extract[String]
    Consumption(consumption, currentAverageDemand, dateString)
  }
}

case class AverageConsumption(
    movingAverageConsumption: Double,
    cumulativeAverageConsumption: Double,
    nElements: Int,
    timestamp: String,
    unit: String = "W"
) {
  def toJson = write(this)(DefaultFormats)
  def toBytes = ByteString(toJson)
}
