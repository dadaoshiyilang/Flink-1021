package com.atguigu

import com.atguigu.utils.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 通过增量聚合和全窗口聚合求最大最小值
object MinMaxTempWithAggregateAndProcessWindowExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource).map(rd => (rd.id, rd.temperature, rd.temperature))
      .keyBy(_._1).timeWindow(Time.seconds(5)).reduce((r1:(String, Double, Double), r2:(String, Double, Double)) => {
      (r1._1, r1._2.min(r2._2), r1._3.max(r2._3))
    },
      new AssignWindowEndProcessFunction
    )
    stream.print()
    env.execute()
  }

  case class MinMaxTemp(id:String, min:Double, max:Double, ends:Long)

  class AssignWindowEndProcessFunction extends ProcessWindowFunction[(String,Double,Double),MinMaxTemp,String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Double, Double)], out: Collector[MinMaxTemp]): Unit = {
      val head: (String, Double, Double) = elements.head
      out.collect(MinMaxTemp(key,head._2,head._3, context.window.getEnd))
    }
  }

}
