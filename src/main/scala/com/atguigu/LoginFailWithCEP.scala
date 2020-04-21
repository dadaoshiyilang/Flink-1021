package com.atguigu


import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCEP {

  case class LoginEvent(userId: String, ip: String, eventType: String, ts: String)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      .fromElements(
        LoginEvent("1", "0.0.0.0", "fail", "1"),
        LoginEvent("1", "0.0.0.0", "fail", "2"),
        LoginEvent("1", "0.0.0.0", "fail", "3"),
        LoginEvent("1", "0.0.0.0", "fail", "4")
      )
      .assignAscendingTimestamps(_.ts.toLong * 1000)
      .keyBy(_.userId)

   val pattern =  Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("middle")
      .where(_.eventType == "fail")
      .next("end")
      .where(_.eventType == "fail")
      .within(Time.seconds(5))

    val patternStream = CEP.pattern(stream, pattern)

    patternStream.select(pattern => {
      val first = pattern.getOrElse("begin", null).iterator.next()
      val second = pattern.getOrElse("middle", null).iterator.next()
      val third = pattern.getOrElse("end", null).iterator.next()
      (first.ts, second.ts, third.ts)
    }).print()

    env.execute()
  }
}