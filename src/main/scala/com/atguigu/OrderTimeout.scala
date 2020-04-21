package com.atguigu

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import scala.collection.Map

object OrderTimeout {

  case class OrderEvent(orderId: String, eventType: String, eventTime: String)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventStream = env
      .fromCollection(List(
        OrderEvent("1", "create", "1558430842"),
        OrderEvent("2", "create", "1558430843"),
        OrderEvent("2", "pay", "1558430844"),
        OrderEvent("3", "pay", "1558430942"),
        OrderEvent("4", "pay", "1558430943")
      ))
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy(_.orderId)

    val pattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .next("next")
      .where(_.eventType == "pay")
      .within(Time.seconds(5))

    val output = new OutputTag[OrderEvent]("timeout")


    val patternStream = CEP.pattern(orderEventStream, pattern)

    val timeoutFunction = (map:Map[String, Iterable[OrderEvent]],timestamp:Long,out:Collector[OrderEvent]) => {
      val orderStart = map.get("begin").get.head
      out.collect(orderStart)
    }
    
    val selectFunction = (map:Map[String,Iterable[OrderEvent]], out:Collector[OrderEvent]) => {

      val create = map.get("begin").get.head
      val pay = map.get("next").get.head
      out.collect(pay)
      
    }
    
    val timeoutOrder = patternStream.flatSelect(output)(timeoutFunction)(selectFunction)

    timeoutOrder.getSideOutput(output).print()

    timeoutOrder.print()

    env.execute()
  }
}
