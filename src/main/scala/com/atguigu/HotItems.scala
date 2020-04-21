package com.atguigu

import java.sql.Timestamp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer

object HotItems {

  case class UserBehavior(userId:Long, itemId:Long, categoryId:Int,behavior:String,timestamp:Long)

  case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.readTextFile("D:\\workspace\\Flink-1021\\src\\main\\resources\\UserBehavior.csv")
      .map(r=>{
        val arr = r.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong,arr(2).toInt,arr(3),arr(4).toLong*1000)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg, new WindowResult)
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))

    stream.print()
    env.execute()
  }

  class CountAgg extends AggregateFunction[UserBehavior,Long,Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1

  }

  class WindowResult extends ProcessWindowFunction[Long, ItemViewCount,Long,TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key,context.window.getEnd, elements.head))
    }
  }

  class  TopNHotItems(topSize:Int) extends KeyedProcessFunction[Long,ItemViewCount,String] {

    lazy val itemState = getRuntimeContext.getListState{
      new ListStateDescriptor[ItemViewCount]("items",Types.of[ItemViewCount])
    }

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {

      itemState.add(value)

      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

      val allItems = new ListBuffer[ItemViewCount]()
      import  scala.collection.JavaConversions._
      for (item <- itemState.get()) {
        allItems += item
      }
      itemState.clear()

      val sortedItems = allItems.sortBy(-_.count).take(topSize)

      val result = new StringBuilder
      result
        .append("=================================\n")
        .append("时间：")
        .append(new Timestamp(timestamp - 1))
        .append("\n")

      for (i <- sortedItems.indices) {
        val currentItem = sortedItems(i)
        result
          .append("No")
          .append(i + 1)
          .append(":")
          .append(" 商品ID = ")
          .append(currentItem.itemId)
          .append(" 浏览量 = ")
          .append(currentItem.count)
          .append("\n")
      }
      result
        .append("==================================\n\n")
      Thread.sleep(1000)
      out.collect(result.toString())
    }
  }
}
