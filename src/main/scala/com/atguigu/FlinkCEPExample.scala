package com.atguigu

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkCEPExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.fromElements(
      LoginEvent("1", "192.168.0.1", "fail", "1558430842"),
      LoginEvent("1", "192.168.0.2", "fail", "1558430843"),
      LoginEvent("1", "192.168.0.3", "fail", "1558430844"),
      LoginEvent("2", "192.168.10.10", "success", "1558430845")
    ).assignAscendingTimestamps(_.eventTime.toLong * 1000).keyBy(_.userId)

    val loginFailPattern = Pattern
      .begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("middle")
      .where(_.eventType == "fail")
      .next("end")
      .where(_.eventType == "fail")
      .within(Time.seconds(10))

    val patternStream = CEP.pattern(stream, loginFailPattern)

    val output = patternStream
      .select((pattern: scala.collection.Map[String, Iterable[LoginEvent]]) => {
        val first = pattern.getOrElse("begin", null).iterator.next()
        val second = pattern.getOrElse("middle", null).iterator.next()
        val third = pattern.getOrElse("end", null).iterator.next()
        (first.ip, second.ip, third.ip)
      })
    output.print()
    env.execute()
  }
  case class LoginEvent(userId: String, ip: String, eventType: String, eventTime: String)
}