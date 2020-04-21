package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SumExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream: DataStream[(String, Int)] = env.addSource(new SensorSource).keyBy(_.id).timeWindow(Time.seconds(5)).
      aggregate(new MySum)
    stream.print()
    env.execute()
  }

  class MySum extends AggregateFunction[SensorReading,(String,Int),(String,Int)] {

    override def createAccumulator(): (String, Int) = ("", 0)

    override def add(in: SensorReading, acc: (String, Int)): (String, Int) = {
      (in.id, 1 + acc._2)
    }

    override def getResult(acc: (String, Int)): (String, Int) = {
      acc
    }

    override def merge(acc: (String, Int), acc1: (String, Int)): (String, Int) = {
      (acc._1, acc._2 + acc1._2)
    }
  }
}
