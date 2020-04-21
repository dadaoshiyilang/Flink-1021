package com.atguigu

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UserBehaviorUVWithKeyBy {

  case class UserBehavior(userId:Long, itemId:Long, categoryId:Int, behavior:String, timestamp:Long)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.readTextFile("D:\\workspace\\Flink-1021\\src\\main\\resources\\UserBehavior.csv")
      .map( rd => {
        val arr = rd.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong*1000)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.userId)
      .timeWindow(Time.hours(1))
      .process(new WindowResult)
      .keyBy(_._3)
      .process(new UVKeyedProcess)
    stream.print()
    env.execute()
  }

  class WindowResult extends ProcessWindowFunction[UserBehavior,(Long,Long,Long),Long,TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[UserBehavior], out: Collector[(Long, Long, Long)]): Unit = {
      out.collect((key, 1L, context.window.getEnd))
    }
  }

  class  UVKeyedProcess extends KeyedProcessFunction[Long,(Long,Long,Long), String] {

    lazy val uv = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("uv",classOf[Long])
    )

    override def processElement(value: (Long, Long, Long), ctx: KeyedProcessFunction[Long, (Long, Long, Long), String]#Context, out: Collector[String]): Unit = {
      uv.update(uv.value() + 1) // 每次加1
      ctx.timerService().registerEventTimeTimer(value._3 + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, (Long, Long, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("窗口结束时间是："+(timestamp -1) +"的窗口中UV数据是：" +uv.value())
      uv.clear()
    }
  }
}