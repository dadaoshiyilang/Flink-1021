package com.atguigu

import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UVWithBloomFilter1 {
  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env
      .addSource(new FlinkKafkaConsumer011[String]("hotitems", new SimpleStringSchema(), properties))
      //      .readTextFile("/home/parallels/Flink1021Tutorial/src/main/resources/UserBehavior.csv")
      .map(line => {
      val arr = line.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
    })
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior == "pv")
      .map(r => ("dummyKey", r.userId))
      .keyBy(_._1)
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .trigger(new MyTrigger123)
      .process(new MyProcess)

    stream.print()
    env.execute()
  }

  class MyProcess extends ProcessWindowFunction[(String, Long), (Long, Long), String, TimeWindow] {
    lazy val jedis = new Jedis("hadoop102", 6379)
    lazy val bloom = new Bloom(1 << 29)

    override def process(key: String,
                         context: Context,
                         vals: Iterable[(String, Long)],
                         out: Collector[(Long, Long)]): Unit = {
      val storeKey = context.window.getEnd.toString
      var count = 0L

      if (jedis.hget("UvCountHashTable", storeKey) != null) {
        count = jedis.hget("UvCountHashTable", storeKey).toLong
      }

      val userId = vals.last._2
      val offset = bloom.hash(userId.toString, 61)

      val isExist = jedis.getbit(storeKey, offset)
      if (!isExist) {
        jedis.setbit(storeKey, offset, true)
        jedis.hset("UvCountHashTable", storeKey, (count + 1).toString)
      }



      //      out.collect((count, storeKey.toLong))

    }
  }

  class MyTrigger123 extends Trigger[(String, Long), TimeWindow] {
    override def onEventTime(time: Long,
                             window: TimeWindow,
                             ctx: TriggerContext): TriggerResult = {
      if (ctx.getCurrentWatermark >= window.getEnd) {
        val jedis = new Jedis("hadoop102", 6379)
        val key = window.getEnd.toString
        TriggerResult.FIRE_AND_PURGE
        println(key, jedis.hget("UvCountHashTable", key))
      }
      TriggerResult.CONTINUE
    }
    override def onProcessingTime(time: Long,
                                  window: TimeWindow,
                                  ctx: TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
    override def onElement(element: (String, Long),
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: TriggerContext): TriggerResult = {
      TriggerResult.FIRE_AND_PURGE
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