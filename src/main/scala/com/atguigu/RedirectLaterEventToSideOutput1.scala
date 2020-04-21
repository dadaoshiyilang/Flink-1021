package com.atguigu

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RedirectLaterEventToSideOutput1 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("hadoop102", 9999, '\n')
      .map(rd => {
        val arr = rd.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
        .process(new Keyed)
    stream.print()// 主流
    stream.getSideOutput(new OutputTag[String]("late")).print()
    env.execute()
  }

  class Keyed extends  KeyedProcessFunction[String, (String,Long),(String,Long)] {

    val late = new OutputTag[String]("late")

    override def processElement(i: (String, Long), context: KeyedProcessFunction[String, (String, Long), (String, Long)]#Context, collector: Collector[(String, Long)]): Unit = {

      if (i._2 < context.timerService().currentWatermark()) {
        context.output(late, i._2+"时间的事件迟到了'")
      } else {
        collector.collect(i)
      }
    }
  }

}