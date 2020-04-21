package com.atguigu

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable.Set

object UserBehaviorUV {

  case class UserBehavior(userId:Long, itemId:Long, categoryId:Int, behavior:String, timestamp:Long)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.readTextFile("D:\\workspace\\Flink-1021\\src\\main\\resources\\UserBehavior.csv")
      .map(rd => {
        val arr = rd.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timestamp)
      .map(rd => {
        ("dummy", rd.userId)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .process(new WindowUV)
    stream.print()
    env.execute()
  }

  class WindowUV extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {

    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {

      var s = Set[Long]()
      for (ele <- elements) {
        s.add(ele._2)
      }

      out.collect("窗口结束时间是："+context.window.getEnd+"的窗口，UV数据是："+s.size)
    }
  }
}