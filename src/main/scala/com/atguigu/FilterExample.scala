package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object FilterExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    stream.filter(rd => rd.id=="sensor_1").print()

    stream.filter(new MyFilterFunction).print()

    stream.filter(new FilterFunction[SensorReading] {
      override def filter(t: SensorReading): Boolean = t.id == "sensor_1"
    }).print()

    env.execute()
  }

  class MyFilterFunction extends FilterFunction[SensorReading] {
    override def filter(t: SensorReading): Boolean = t.id == "sensor_1"
  }

}
