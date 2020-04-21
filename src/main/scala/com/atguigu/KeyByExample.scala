package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._

object KeyByExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
//    stream.keyBy( r => r.id)
//      .min("temperature").print()

    val inputStream: DataStream[(Int, Int, Int)] = env.fromElements(
      (1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3))

    val resultStream: DataStream[(Int, Int, Int)] = inputStream
      .keyBy(0)
      .sum(1)
    resultStream.print()

    env.execute()
  }
}