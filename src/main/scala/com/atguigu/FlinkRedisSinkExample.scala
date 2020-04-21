package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object FlinkRedisSinkExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()
    stream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))
    env.execute()
  }
  class MyRedisMapper extends RedisMapper[SensorReading] {

    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
    }
    override def getValueFromData(t: SensorReading): String = t.temperature.toString

    override def getKeyFromData(t: SensorReading): String = t.id
  }
}
