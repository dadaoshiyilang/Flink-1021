package com.atguigu

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._

object HotItemsFLinkTableAndSQL {

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

    val table = tEnv.fromDataStream(stream, 'timestamp.rowtime,'itemId)

    val t = table.window(Tumble over 60.minutes on 'timestamp as 'w)
        .groupBy('itemId, 'w)
        .aggregate('itemId.count as 'icount)
        .select('itemId, 'icount, 'w.end as 'windowEnd)
        .toAppendStream[(Long, Long, Timestamp)]

    tEnv.createTemporaryView("topn", t, 'itemId, 'icount, 'windowEnd)

    tEnv.sqlQuery(
      """
        | select * from (select *, ROW_NUMBER() over (partition by windowEnd order by icount desc) as row_num from topn)
        | where row_num <=3
      """.stripMargin
    ).toRetractStream[(Long, Long, Timestamp, Long)]
        .print()

    env.execute()

  }
}
