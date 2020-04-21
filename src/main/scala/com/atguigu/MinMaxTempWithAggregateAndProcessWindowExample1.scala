package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 通过增量聚合和全窗口聚合求最大最小值
object MinMaxTempWithAggregateAndProcessWindowExample1 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
      .keyBy(_.id).timeWindow(Time.seconds(5))
        .aggregate(new Agg, new WindowProcess)
    stream.print()
    env.execute()
  }

  case class MinMaxTemp(id:String, min:Double, max:Double, ends:Long)

  class Agg extends AggregateFunction[SensorReading,(Double,Double),(Double,Double)] {
    override def createAccumulator(): (Double, Double) = {
      (Double.MaxValue,Double.MinValue)
    }

    override def add(in: SensorReading, acc: (Double, Double)): (Double, Double) = {
      (in.temperature.min(acc._1), in.temperature.max(acc._2))
    }

    override def getResult(acc: (Double, Double)): (Double, Double) = {
      acc
    }

    override def merge(acc: (Double, Double), acc1: (Double, Double)): (Double, Double) = {
      (acc._1.min(acc1._1),acc._2.max(acc1._2))
    }
  }

  class WindowProcess extends ProcessWindowFunction[(Double,Double),MinMaxTemp,String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(Double, Double)], out: Collector[MinMaxTemp]): Unit = {
      val minMaxValue: (Double, Double) = elements.head
      out.collect(MinMaxTemp(key,minMaxValue._1,minMaxValue._2, context.window.getEnd))
    }
  }
}
