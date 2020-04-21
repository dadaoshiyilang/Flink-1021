package com.atguigu

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment._

object UnionExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val s1: DataStreamSource[Int] = env.fromElements(1)

    val s2: DataStreamSource[Int] = env.fromElements(2)

    val s3: DataStreamSource[Int] = env.fromElements(3)

    s1.union(s2, s3).print()

    env.execute()
  }
}