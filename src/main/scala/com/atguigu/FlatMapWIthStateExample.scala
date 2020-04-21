package com.atguigu

import com.atguigu.RichFlatMapStateExample.TemperatureAlertFunction
import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._

object FlatMapWIthStateExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .addSource(new SensorSource)
      .keyBy(_.id)
        .flatMapWithState[(String, Double, Double),Double] {
      case (in: SensorReading,None) => (List.empty, Some(in.temperature))
      case (r:SensorReading,lastTemp:Some[Double]) =>
        val tempDiff = (r.temperature - lastTemp.get).abs
        if (tempDiff > 1.7) {
          (List((r.id, r.temperature, tempDiff)), Some(r.temperature))
        } else {
          (List.empty, Some(r.temperature))
        }
    }
//      .flatMapWithState[(String, Double, Double), Double] {
//      case (in: SensorReading, None) => (List.empty, Some(in.temperature))
//      case (r: SensorReading, lastTemp: Some[Double]) =>
//        val tempDiff = (r.temperature - lastTemp.get).abs
//        if (tempDiff > 1.7) {
//          (List((r.id, r.temperature, tempDiff)), Some(r.temperature))
//        } else {
//          (List.empty, Some(r.temperature))
//        }
//    }
    stream.print()
    env.execute()

  }

}
