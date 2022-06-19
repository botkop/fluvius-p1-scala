package p1.raw

import akka.actor.ActorSystem
import akka.stream.scaladsl.Tcp
import akka.stream.scaladsl.Flow
import akka.NotUsed
import akka.util.ByteString
import akka.stream.scaladsl.Source
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Sink
import akka.stream.alpakka.mqtt.MqttConnectionSettings
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import akka.stream.alpakka.mqtt.MqttMessage
import scala.concurrent.Future
import akka.stream.alpakka.mqtt.scaladsl.MqttSource
import akka.stream.alpakka.mqtt.MqttSubscriptions
import akka.stream.alpakka.mqtt.MqttQoS
import akka.stream.scaladsl.Keep
import akka.Done
import akka.stream.scaladsl.Framing
import org.json4s._
import org.json4s.native.Serialization._
import org.json4s.native.Serialization

import akka.stream.scaladsl._
import akka.util.ByteString
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, ContentTypes}
import akka.http.scaladsl.server.Directives._
import scala.util.Random
import scala.io.StdIn
import akka.stream.stage.GraphStage
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.Attributes
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.OutHandler
import akka.stream.stage.InHandler
import akka.actor.Actor
import akka.actor.Props

import scala.concurrent.duration.DurationInt

object MqttConsumer extends App {

  implicit val system: ActorSystem = ActorSystem("MqttConsumer")
  implicit val executionContext = system.dispatcher
  implicit val formats = Serialization.formats(NoTypeHints)

  val mqttHost = "192.168.0.247"
  val mqttPort = 1883
  val mqttTopic = "home/p1"

  val connectionSettings = MqttConnectionSettings(
    f"tcp://${mqttHost}:${mqttPort}",
    "p1-mqtt-consumer",
    new MemoryPersistence
  )

  val mqttSource: Source[MqttMessage, Future[Done]] =
    MqttSource.atMostOnce(
      connectionSettings,
      MqttSubscriptions(Map(mqttTopic -> MqttQoS.AtLeastOnce)),
      bufferSize = 20
    )

  val keyMap = Map(
    "0-0:1.0.0" -> "timestamp",
    // "0-0:96.3.10" -> "switchElectricity",
    // "0-1:24.4.0" -> "switchGas",
    // "0-0:96.1.1" -> "meterSerialElectricity",
    // "0-1:96.1.1" -> "meterSerialGas",
    "0-0:96.14.0" -> "currentRate",
    "1-0:1.8.1" -> "rate1TotalConsumption",
    "1-0:1.8.2" -> "rate2TotalConsumption",
    "1-0:2.8.1" -> "rate1TotalProduction",
    "1-0:2.8.2" -> "rate2TotalProduction",
    "1-0:21.7.0" -> "l1Consumption",
    "1-0:41.7.0" -> "l2Consumption",
    "1-0:61.7.0" -> "l3Consumption",
    "1-0:1.7.0" -> "allPhasesConsumption",
    "1-0:22.7.0" -> "l1Production",
    "1-0:42.7.0" -> "l2Production",
    "1-0:62.7.0" -> "l3Production",
    "1-0:2.7.0" -> "allPhasesProduction",
    "1-0:32.7.0" -> "l1Voltage",
    "1-0:52.7.0" -> "l2Voltage",
    "1-0:72.7.0" -> "l3Voltage",
    "1-0:31.7.0" -> "l1Current",
    "1-0:51.7.0" -> "l2Current",
    "1-0:71.7.0" -> "l3Current"
    // "0-1:24.2.3" -> "gasConsumption" // format: 0-1:24.2.3(211106072002W)(01581.796*m3)
  )

  def adjustValue(key: String, value: String) = {
    val pattern = "([\\d\\.]+W?)(?:\\*(\\S+))?".r
    val pattern(num, uom) = value

    key match {
      case "timestamp" =>
        Map("value" -> num.dropRight(1).toDouble, "uom" -> uom)
      case _ =>
        Map("value" -> num.toDouble, "uom" -> uom)
    }
  }

  val transformer = Flow[String].map { s: String =>
    val m = s
      .split("\r")
      .flatMap { entry =>
        val kvPattern = "([01]\\-[01]\\:\\d+.\\d+.\\d+)\\((\\S*)\\)".r
        val crcPattern = "!([A-F0-9]+)".r
        entry match {
          case kvPattern(k, v) =>
            keyMap.get(k).map(f => (f, adjustValue(f, v)))
          // case crcPattern(v) => Some("crc", v)
          case _ => None
        }
      }
      .toMap
    write(m)
  }

  val messages = mqttSource
    .map(_.payload)
    .via(
      Framing.delimiter(
        ByteString("/FLU5\\253770234_A\r\r"),
        maximumFrameLength = 2048,
        allowTruncation = false
      )
    )
    .map(_.utf8String)
    .via(transformer)
    .log("error logging")


  case class Put(message: String)
  case object Get

  class MessageCache extends Actor {
    var lastMessage = "{}"
    override def receive: Receive = {
      case Put(message) =>
        lastMessage = message
      case Get =>
        sender() ! lastMessage
      case z =>
    }
  }

  val cache = system.actorOf(Props[MessageCache](), "cache")
  messages
    .map(s => Put(s))
    .to(Sink.actorRef(cache, None, _ => None))
    .run()

  implicit val timeout: akka.util.Timeout = 1.second
  def getMessageFromCache =
    Source.fromIterator(() => Iterator.single(Get)).ask[String](cache)

  val route =
    path("p1") {
      get {
        complete(
          HttpEntity(
            ContentTypes.`application/json`,
            // transform each number to a chunk of bytes
            getMessageFromCache.map(n => ByteString(n))
          )
        )
      }
    }

  val bindingFuture = Http().newServerAt("0.0.0.0", 5080).bind(route)

}
