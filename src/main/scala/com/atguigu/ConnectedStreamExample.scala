package com.atguigu

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ConnectedStreamExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val s1 = env.fromElements(
      (1, "huo"),
      (2, "yan")
    )
    val s2 = env.fromElements(
      (1, "shui"),
      (2, "shui")
    )

    s1.keyBy(_._1).connect(s2.keyBy(_._1)).flatMap(new MyFlatMap).print()

    //s1.connect(s2).keyBy(0,0).flatMap(new MyFlatMap).print()

    env.execute()
  }

  class MyFlatMap extends CoFlatMapFunction[(Int, String),(Int, String),String] {

    override def flatMap1(in1: (Int, String), collector: Collector[String]): Unit = {

      if (in1._2 !="shui") {
        collector.collect("来自第一条流"+in1._2+" Key:"+in1._1)
      }
    }

    override def flatMap2(in2: (Int, String), collector: Collector[String]): Unit = {

      if (in2._2 !="shui") {
        collector.collect("来自第二条流"+in2._2+" Key:"+in2._1)
      }
    }
  }

}
