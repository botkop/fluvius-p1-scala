package p1.mqtt

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt.MqttConnectionSettings
import akka.stream.alpakka.mqtt.MqttMessage
import akka.stream.alpakka.mqtt.MqttQoS
import akka.stream.alpakka.mqtt.MqttSubscriptions
import akka.stream.alpakka.mqtt.scaladsl.MqttSink
import akka.stream.alpakka.mqtt.scaladsl.MqttSource
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import net.liftweb.json.Serialization.write
import net.liftweb.json._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import akka.stream.Attributes

object TimedEnergy extends App with LazyLogging {

  logger.info("starting")

  implicit val system: ActorSystem = ActorSystem("timed-energy")
  implicit val executionContext = system.dispatcher

  val config = ConfigFactory.load()
  val mqttUrl = config.getString("mqtt.url")
  val mqttUser = config.getString("mqtt.user")
  val mqttPassword = config.getString("mqtt.password")
  val mqttSourceTopic = config.getString("mqtt.timed.topics.source")
  val mqttTargetTopic = config.getString("mqtt.timed.topics.target")
  val interval = config.getInt("mqtt.timed.interval")
  val consumerId = config.getString("mqtt.timed.consumer-id")
  val producerId = config.getString("mqtt.timed.producer-id")

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

  mqttSource
    .map(msg => parse(msg.payload.utf8String))
    .map(Energy.apply)
    .groupedWithin(interval + 5, FiniteDuration(interval, TimeUnit.SECONDS))
    .map { list =>
      val production = list.map(_.production).sum / list.size
      val consumption = list.map(_.consumption).sum / list.size
      Energy(production, consumption)
    }
    .log("timed-energy")
    .addAttributes(Attributes.logLevels(onElement = Attributes.LogLevels.Info))
    .map(energy => MqttMessage(mqttTargetTopic, energy.toBytes))
    .runWith(mqttSink)
    // .runForeach(msg => println(msg.payload.utf8String))
}

case class Energy(production: Double, consumption: Double) {
  def toJson = write(this)(DefaultFormats)
  def toBytes = ByteString(toJson)
}

case object Energy {
  implicit val formats = DefaultFormats
  def apply(json: JValue): Energy = {
    val production = (json \ "CURRENT_ELECTRICITY_DELIVERY" \ "value").extract[Double]
    val consumption = (json \ "CURRENT_ELECTRICITY_USAGE" \ "value").extract[Double]
    Energy(production, consumption)
  }
}
