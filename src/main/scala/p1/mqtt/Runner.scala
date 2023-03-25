package p1.mqtt

import scala.collection.parallel.CollectionConverters._


object Runner extends App {

    val l = List(TimedEnergy, MovingAverageConsumption)
    l.par.map(_.main(Array.empty))
  
}
