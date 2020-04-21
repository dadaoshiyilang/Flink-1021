package com.atguigu

import org.apache.flink.streaming.api.scala._
import com.atguigu.utils.{SensorReading, SensorSource}

object SensorSourceExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    stream.print()

    env.execute()

  }

}
