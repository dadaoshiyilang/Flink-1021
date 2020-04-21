package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RichFlatMapStateExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .flatMap(new TemperatureAlertFunction(1.7))
    stream.print()
    env.execute()
  }

  class TemperatureAlertFunction(d: Double) extends RichFlatMapFunction[SensorReading, (String,Double,Double)] {

    lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp",Types.of[Double]))

    override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
      val lastTemp = lastTempState.value()
      val tempDiff = (in.temperature -lastTemp).abs
      if (tempDiff > d) {
        collector.collect((in.id, in.temperature,tempDiff))
      }
      lastTempState.update(in.temperature)
    }
  }
}
