package com.atguigu

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object ApacheLogAnalysls {

  case class  ApacheLogEvent(ip:String, userId:String, eventTime:Long, method:String,url:String)

  case class  UrlViewCount(url:String,windowEnd:Long,count:Long)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 如果想要追求极限的低延迟，请使用处理时间，使用滚动窗口
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.readTextFile("D:\\workspace\\Flink-1021\\src\\main\\resources\\apachelog.txt")
        .map(rd => {
          val arr = rd.split(" ")
          val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")

          val ts = dateFormat.parse(arr(3)).getTime
          ApacheLogEvent(arr(0), arr(2), ts, arr(5), arr(6))
        }).assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
          override def extractTimestamp(element: ApacheLogEvent): Long = {
            element.eventTime
          }
        }
    )
        .keyBy(_.url)
        .timeWindow(Time.seconds(60), Time.seconds(5))
        .aggregate(new CountAgg, new WindowResult)
        .keyBy(_.windowEnd)
        .process(new TopNHotUrls)
    stream.print()
    env.execute()
  }

  class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {

    override def createAccumulator(): Long = {
      0L
    }

    override def add(in: ApacheLogEvent, acc: Long): Long = {
      acc + 1
    }

    override def getResult(acc: Long): Long = {
      acc
    }

    override def merge(acc: Long, acc1: Long): Long = {
      acc + acc1
    }
  }

  class  WindowResult extends ProcessWindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(key, context.window.getEnd, elements.head))
    }
  }

  class  TopNHotUrls extends  KeyedProcessFunction[Long, UrlViewCount, String] {
    var urls:ListState[UrlViewCount] =_

    override def open(parameters: Configuration): Unit = {
      urls = getRuntimeContext.getListState(
        new ListStateDescriptor[UrlViewCount]("urls", Types.of[UrlViewCount])
      )
    }

    override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
      urls.add(value)

      ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

      val allUrls = ListBuffer[UrlViewCount]()
      import scala.collection.JavaConversions._

      for (urlView <- urls.get()) {
        allUrls +=urlView
      }
      urls.clear()

      val sortedUrlViews = allUrls.sortBy(-_.count).take(1)
      out.collect("窗口结束时间是："+new Timestamp(timestamp - 100)+"的窗口，访问量最大的url是：" +sortedUrlViews(0).url)
    }
  }
}
