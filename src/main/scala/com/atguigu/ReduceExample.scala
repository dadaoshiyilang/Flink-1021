package com.atguigu

import org.apache.flink.streaming.api.scala._

object ReduceExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream: DataStream[(String, List[String])] = env.fromElements(("en",List("tea")),("fr",List("vin")),("en",List("cake")))


    stream.keyBy(0).reduce((x,y) => (x._1, x._2:::y._2)).print()

//    stream.keyBy(0).reduce((x, y) => (x._1, x._2:::y._2)).print()

    env.execute()

  }

}
