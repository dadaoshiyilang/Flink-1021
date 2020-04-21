package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object MinTempWindowExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

//    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
//    val rsStream: DataStream[(String, Double)] = stream.map(rd => (rd.id, rd.temperature)).keyBy(_._1).
//      timeWindow(Time.seconds(10), Time.seconds(5)).reduce((r1, r2) => {
//      (r1._1, r1._2.min(r2._2))
//    })
//    rsStream.print()

    val stream = env.addSource(new SensorSource)

    stream.map(rd =>(rd.id, rd.temperature)).keyBy(_._1).timeWindow(Time.seconds(5)).
      reduce((r1, r2) => (r1._1, r1._2.min(r2._2))).print()

    env.execute()
  }
}
