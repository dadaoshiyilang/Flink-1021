package com.atguigu

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object EventTimeOnTimer {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("hadoop102", 9999, '\n')
      .map(rd => {
        val arr = rd.split(" ")
        (arr(0), arr(1).toLong * 1000)
      }).assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .process(new MyKeyed)
    stream.print()
    env.execute()
  }

  class MyKeyed extends KeyedProcessFunction[String, (String,Long), String] {
    override def processElement(i: (String, Long), context: KeyedProcessFunction[String, (String, Long), String]#Context, collector: Collector[String]): Unit = {
      val ts = i._2 + 5* 1000L
      context.timerService().registerEventTimeTimer(ts)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("回调函数触发执行!")
    }
  }

}
