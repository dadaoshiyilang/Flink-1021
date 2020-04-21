package com.atguigu

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.java.tuple.Tuple2

object Tuple2Example {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(
      Tuple2.of("admin", 17),
      Tuple2.of("zhong", 9)
    )
    stream.filter(_.f1 >10).print()

    env.execute()

  }

}
