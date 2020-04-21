package com.atguigu

import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object LoginFailWithoutCEP {

  case class LoginEvent(userId:String, ip:String, eventType:String, ts:String)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.fromElements(
      LoginEvent("1", "0.0.0.0", "fail", "1"),
      LoginEvent("1", "0.0.0.0", "fail", "2"),
      LoginEvent("1", "0.0.0.0", "fail", "3"),
      LoginEvent("1", "0.0.0.0", "fail", "4")
    ).assignAscendingTimestamps(_.ts.toLong * 1000)
      .keyBy(_.userId)
      .process(new MatchFunction)

    stream.print()
    env.execute()
  }

  class MatchFunction extends  KeyedProcessFunction[String,LoginEvent,String] {
    lazy  val logins = getRuntimeContext.getListState(
      new ListStateDescriptor[LoginEvent]("logins", classOf[LoginEvent])
    )

    lazy val ts = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("ts", classOf[Long])
    )

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[String, LoginEvent, String]#Context, out: Collector[String]): Unit = {

      if (value.eventType == "fail") {
        logins.add(value)
        if (ts.value() == 0L) {
          val timestamp = value.ts.toLong * 1000 + 5000L
          ts.update(timestamp)
          ctx.timerService().registerEventTimeTimer(timestamp)
        }
      }

      if (value.eventType == "success") {
        logins.clear()
        ctx.timerService().deleteEventTimeTimer(ts.value())
        ts.clear()
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, LoginEvent, String]#OnTimerContext, out: Collector[String]): Unit = {

      val allLogins = ListBuffer[LoginEvent]()
      import scala.collection.JavaConversions._
      for (elem <- logins.get) {
        allLogins.add(elem)
      }
      logins.clear()
      if (allLogins.length > 2) {
        out.collect("5秒以内连续3次登录失败！")
        ts.clear()
      }
    }
  }
}
