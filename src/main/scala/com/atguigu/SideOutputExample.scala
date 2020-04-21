package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
      .process(new FreezingMonitor)
    //    stream.print() // 打印主流
    // stream.getSideOutput(new OutputTag[String]("freezing-alarms")).print() // 打印侧输出流
    stream.getSideOutput(new OutputTag[String]("freezing-alarms")).print()

    env.execute()
  }

  // 用来处理没有keyby过的流
  class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {

    lazy val freezingAlarmOutPut = new OutputTag[String]("freezing-alarms")

    override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {

      if (i.temperature < 32.0) {
        context.output(freezingAlarmOutPut, s"传感器${i.id}低温警告")
      }
      collector.collect(i)
    }
  }
}