package com.atguigu

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object RichMap {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Int] = env.fromElements(1,2,3)

    stream.map(new MyMap).print()

    env.execute()
  }

  class MyMap extends RichMapFunction[Int,Int] {

    override def open(parameters: Configuration): Unit = {
      println("enter open function")
      val idx = getRuntimeContext.getIndexOfThisSubtask
      println("idx: " + idx)
    }
    override def map(in: Int): Int = {
      in + 1
    }
    override def close() {
      println("enter close function")
    }
  }
}
