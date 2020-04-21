package com.atguigu

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._

object HotItemsFLinkSQL {

  case class UserBehavior(userId:Long, itemId:Long, categoryId:Int, behavior:String, timestamp:Long)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.readTextFile("D:\\workspace\\Flink-1021\\src\\main\\resources\\UserBehavior.csv")
      .map(rd => {
        val arry = rd.split(",")
        UserBehavior(arry(0).toLong, arry(1).toLong, arry(2).toInt, arry(3), arry(4).toLong)
      }).filter(_.behavior =="pv")
      .assignAscendingTimestamps(_.timestamp)

    // 创建临时表
    tEnv.createTemporaryView("t", stream,'itemId, 'timestamp.rowtime as 'ts)

    // 滑动窗口 TUMBLE 窗口长度TUMBLE_END(ts, INTERVAL '1' HOUR)
    // 滚动窗口 HOP
    tEnv.sqlQuery(
      """
        |SELECT *
        |FROM (
        |      SELECT *,
        |             ROW_NUMBER() OVER(PARTITION BY windowEnd ORDER BY icount DESC) as row_num
        |             FROM (
        |                   SELECT count(itemId) as icount, HOP_END(ts, INTERVAL '5'MINUTE,INTERVAL '1' HOUR) as windowEnd
        |                   FROM t GROUP BY HOP(ts, INTERVAL '5'MINUTE,INTERVAL '1' HOUR), itemId
        |                   )
        | )
        | WHERE row_num <= 3
      """.stripMargin
    ).toRetractStream[(Long, Timestamp, Long)]
      .print()
    env.execute()
  }
}
