package com.atguigu

import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UVWithBloomFilter {

  case class UserBehavior(userId:Long, itemId:Long, categoryId:Int, behavior:String, timestamp:Long)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream =
//      env.readTextFile("D:\\workspace\\Flink-1021\\src\\main\\resources\\UserBehavior.csv")
        env.addSource(new FlinkKafkaConsumer011[String]("hotitems", new SimpleStringSchema(), properties))
      .map( rd => {
        val arr = rd.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong*1000)
      })
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior == "pv")
      .map(rd =>("dummy", rd.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger)
      .process(new MyProcess)

    stream.print()
    env.execute()
  }

  class MyTrigger extends  Trigger[(String,Long),TimeWindow] {
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      // 每来一条数据，处理一条数据
      TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      if (ctx.getCurrentWatermark >= window.getEnd) {
        val key = window.getEnd.toString
        val jedis = new Jedis("hadoop102",6379)
        TriggerResult.FIRE_AND_PURGE
        println(key, jedis.hget("UvCountHashTable", key))
      }
      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {

    }
  }

  class MyProcess extends ProcessWindowFunction[(String, Long), (Long,Long),String, TimeWindow] {
    // redis: 1,用来计数，2,布隆过滤器过滤
    lazy val jedis = new Jedis("hadoop102", 6379)
    lazy val bloom = new Bloom(1 << 29)

    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[(Long, Long)]): Unit = {

      val storeKey = context.window.getEnd.toString

      var count = 0L
      println(">>>>>>"+ jedis.hget("UvCountHashTable", storeKey))
      if (jedis.hget("UvCountHashTable", storeKey) != null) {
        count = jedis.hget("UvCountHashTable", storeKey).toLong
      }

      val userId = elements.last._2.toString
      val offset = bloom.hash(userId, 61)

      val isExist = jedis.getbit(storeKey, offset)
      println("isExist:" +isExist)

      if (!isExist) {
        jedis.setbit(storeKey, offset, true)
        jedis.hset("UvCountHashTable", storeKey, (count + 1).toString)
      }
    }
  }

  class Bloom(size: Long) extends Serializable {
    private val cap = size

    def hash(value: String, seed: Int): Long = {
      var result = 0
      for (i <- 0 until value.length) {
        result = result * seed + value.charAt(i)
      }
      (cap - 1) & result
    }
  }
}