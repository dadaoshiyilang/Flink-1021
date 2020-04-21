package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoProcessFunctionExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val flierSwitches = env.fromCollection(Seq(("sensor_2", 5 * 1000L), ("sensor_7", 10 * 1000L)))
      .keyBy(_._1)

    val stream = env.addSource(new SensorSource).keyBy(_.id)
    val readings = stream.connect(flierSwitches).process(new ReadingFilter)
    readings.print()
    env.execute()
  }

  class ReadingFilter extends CoProcessFunction[SensorReading, (String,Long),SensorReading] {
    lazy val forwardingEnabled = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("switch", Types.of[Boolean])
    )
    override def processElement1(in1: SensorReading, context: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
      if (forwardingEnabled.value()) {
        collector.collect(in1)
      }
    }

    override def processElement2(in2: (String, Long), context: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
      forwardingEnabled.update(true)
      val ts = context.timerService().currentProcessingTime() + in2._2
      context.timerService().registerProcessingTimeTimer(ts)
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {
      forwardingEnabled.clear()
    }
  }
}
