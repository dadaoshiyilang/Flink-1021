package com.atguigu

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object UserBehaviorPV {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.readTextFile("D:\\workspace\\Flink-1021\\src\\main\\resources\\UserBehavior.csv")
      .map(rd => {
        val arr = rd.split(",")
        (arr(0), arr(1), arr(2), arr(3), arr(4).toLong * 1000)
      })
      .filter(_._4 == "pv")
      .assignAscendingTimestamps(_._5)
      .map(r => ("dummy", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    stream.print()
    env.execute()
  }
}
