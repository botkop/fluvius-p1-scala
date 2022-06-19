package p1.raw

import akka.stream.scaladsl.Tcp._
import akka.stream.scaladsl._
import scala.concurrent.Future
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext

import org.json4s._
import org.json4s.native.Serialization._
import org.json4s.native.Serialization



object Main extends App {

  implicit val system: ActorSystem = ActorSystem("QuickStart")
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val formats = Serialization.formats(NoTypeHints)

  val host = "192.168.0.247"
  val port = 5500
  val connection = Tcp().outgoingConnection(host, port)

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
      "1-0:71.7.0" -> "l3Current",
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
    val m = s.split("\r\n").toList.flatMap { entry =>
      val kvPattern = "([01]\\-[01]\\:\\d+.\\d+.\\d+)\\((\\S*)\\)".r
      val crcPattern = "!([A-F0-9]+)".r
      entry match {
        case kvPattern(k, v) => 
          keyMap.get(k).map(f => (f, adjustValue(f, v)))
        // case crcPattern(v) => Some("crc", v)
        case _ =>  None
      }
    }
    .toMap
    write(m)
  }

  Source.maybe[ByteString]
    .via(connection)
    .via(Framing.delimiter(ByteString("/FLU5\\253770234_A\r\n\r\n"), maximumFrameLength = 2048, allowTruncation = false))
    .map(_.utf8String)
    .via(transformer)
    .log("error logging")
    .to(Sink.foreach(println))
    .run()

}