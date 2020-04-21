package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._

object TableWindowExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
          override def extractTimestamp(element: SensorReading): Long = element.timestamp
        }
      )

    val settings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)
    val table = tEnv.fromDataStream(stream, 'id, 'temperature, 'timestamp.rowtime)
    table
      .window(Tumble over 10.seconds on 'timestamp as 'w)
      .groupBy('id, 'w) // .keyBy(_.id).timeWindow(Time.seconds(10))
      .select('id, 'id.count)
      .toRetractStream[(String, Long)]
      .print()
    env.execute()
  }
}