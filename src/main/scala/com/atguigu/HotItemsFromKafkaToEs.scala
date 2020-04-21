package com.atguigu

import java.sql.Timestamp
import java.util
import java.util.{HashMap, Properties}

import org.apache.flink.api.common.functions.{AggregateFunction, RuntimeContext}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
import scala.collection.mutable.ListBuffer

object HotItemsFromKafkaToEs {

  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
  case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .addSource(new FlinkKafkaConsumer011[String]("hotitems", new SimpleStringSchema(), properties))
      //        .readTextFile("/home/parallels/Flink1021Tutorial/src/main/resources/UserBehavior.csv")
      .map(r => {
      val arr = r.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
    })
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg, new WindowResult)
      .keyBy(_.windowEnd)
      .process(new TopNHotItems)

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102", 9200))

    val esSinkBuilder = new ElasticsearchSink.Builder[String](
      httpHosts,
      new ElasticsearchSinkFunction[String] {
        override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          val json = new HashMap[String, String]()
          json.put("data", t)
          val indexRequest = Requests
            .indexRequest()
            .index("userbehavior")
            .`type`("topn")
            .source(json)
          requestIndexer.add(indexRequest)
        }
      }
    )

    esSinkBuilder.setBulkFlushMaxActions(1)

    stream.addSink(esSinkBuilder.build())
    stream.print()
    env.execute()
  }
  class TopNHotItems extends KeyedProcessFunction[Long, ItemViewCount, String] {
    lazy val items = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("items", Types.of[ItemViewCount])
    )

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
      items.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allItems = new ListBuffer[ItemViewCount]()
      import scala.collection.JavaConversions._
      for (item <- items.get) {
        allItems += item
      }
      items.clear()

      val sortedItems = allItems.sortBy(-_.count).take(1)
      val ts = new Timestamp(timestamp - 1)
      out.collect("窗口结束时间为：" + ts + "的窗口中浏览量最大的商品是：" + sortedItems(0).itemId)
    }
  }
  class WindowResult extends ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key, context.window.getEnd, elements.head))
    }
  }
  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

}
