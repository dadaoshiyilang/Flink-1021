package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object MinMaxTempExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .addSource(new SensorSource)
      .keyBy(_.id)
//      .window(TumblingEventTimeWindows.of(Time.seconds(5)))  // 等价于timeWindow
      .timeWindow(Time.seconds(5))
      .process(new MyMinMax)
    stream.print()
    env.execute()
  }

  case class MinMaxTemp(id:String,min:Double,max:Double,endTs:Long)

  class MyMinMax extends ProcessWindowFunction[SensorReading,MinMaxTemp,String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[MinMaxTemp]): Unit = {

      val temps = elements.map(_.temperature)
      val endTime = context.window.getEnd
      out.collect(MinMaxTemp(key, temps.min,temps.max, endTime))
    }
  }
}
