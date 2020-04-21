package com.atguigu

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object RedirectLaterEventToSideOutput {

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
      .timeWindow(Time.seconds(10))
      .sideOutputLateData(new OutputTag[(String, Long)]("late"))// 侧输出流
      .process(new CountFunction)
    stream.print()// 主流
    stream.getSideOutput(new OutputTag[(String,Long)]("late")).print()
    env.execute()
  }

  class CountFunction extends ProcessWindowFunction[(String, Long),String,String,TimeWindow] {

    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect("共有" + elements.size +"个元素")
    }
  }
}