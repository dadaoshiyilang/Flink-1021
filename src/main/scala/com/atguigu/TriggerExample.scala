package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TriggerExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource).keyBy(_.id).timeWindow(Time.seconds(10))
      .trigger(new MyTrigger)
      .process(new MyProcessWindow)
    stream.print()
    env.execute()
  }

  class MyTrigger extends Trigger[SensorReading,TimeWindow] {

    override def onElement(t: SensorReading, l: Long, w: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {

      // 初始化分区状态
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("firstSeen", Types.of[Boolean])
      )
      if (!firstSeen.value()) {
        // 第一条元素
        val t = ctx.getCurrentProcessingTime + (1000 -(ctx.getCurrentProcessingTime % 1000))
        ctx.registerProcessingTimeTimer(t)
        ctx.registerProcessingTimeTimer(w.getEnd)
        firstSeen.update(true)
      }
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, w: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {

      if (time == w.getEnd) {
        TriggerResult.FIRE_AND_PURGE // 触发窗口计算并且清空窗口中的元素
      } else {
        val t = ctx.getCurrentProcessingTime + (1000 -(ctx.getCurrentProcessingTime % 1000))
        if (t < w.getEnd) {
          ctx.registerProcessingTimeTimer(t) // 回调函数还是onProcessingTime
        }
        TriggerResult.FIRE // 触发窗口计算，不清空
      }

    }

    override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
      val firstSeen = triggerContext.getPartitionedState(
        new ValueStateDescriptor[Boolean]("firstSeen", Types.of[Boolean])
      )
      firstSeen.clear()
    }
  }

  class MyProcessWindow extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {

      out.collect("时间戳为"+ context.currentProcessingTime +"触发了窗口计算，窗口一共："+elements.size)


    }
  }

}
