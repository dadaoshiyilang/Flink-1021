package com.atguigu

import com.atguigu.utils.SensorSource
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

object TableApiIntroduction {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
    val settings = EnvironmentSettings
      .newInstance()
//      .useOldPlanner()  // pom-->flink-table-planner_2.11
      .useBlinkPlanner() // pom-->flink-table-planner-blink_2.11
      .inStreamingMode()
      .build()
    // 创建流式表的环境
    val tEnv = StreamTableEnvironment.create(env, settings)
    val table = tEnv.fromDataStream(stream)
    table
      .select('id, 'temperature)// 需要导入隐私转换
      .filter("id = 'sensor_1'")
      .toAppendStream[(String, Double)]
      .print()
    env.execute()
  }
}