package com.atguigu

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AllowedLatenessExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      .socketTextStream("hadoop102", 9999, '\n')
      .map(r => {
        val arr = r.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignTimestampsAndWatermarks( // 设置水位线
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(10)) {
        override def extractTimestamp(t: (String, Long)): Long = {
          t._2
        }
      }
    ).keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .allowedLateness(Time.seconds(10)) // 允许迟到的时间
      .process(new UpdatingWindowCountFunction)
    stream.print()
    env.execute()
  }

  class UpdatingWindowCountFunction extends ProcessWindowFunction[(String,Long), String,String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {

      // 当前窗口可见的状态变量
      val isUpdate = context.windowState.getState(
        new ValueStateDescriptor[Boolean]("isUpdate", Types.of[Boolean])
      )

      if (!isUpdate.value()) {
        out.collect("当水位线超过窗口结束时间的时候，触发窗口的第一次聚合计算，共有" + elements.size +"个元素")
        isUpdate.update(true)
      } else {
        out.collect("迟到元素来了！窗口计算结果更新为："+elements.size+"个元素")
      }
    }
  }
}
