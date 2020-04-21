package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedProcessFunctionExample1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .process(new TempIncreaseAlertFunction)
    stream.print()
    env.execute()
  }
  class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {
    // 状态变量; 只会初始化一次; 状态变量会在检查点操作时被保存到状态后端（hdfs，内存，rocksdb） 保存上一个传感器的温度值; 惰性赋值
    // 只是当前的 key 可见；keyed state；单例
    lazy val lastTemp = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("last-temp", Types.of[Double])
    )

    // 保存注册的定时器时间戳的状态变量
    lazy val currentTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

    // 每来一个事件调用一次
    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      // 取出状态变量中保存的上一次温度的值；使用.value()方法进行读取
      // 如果当前元素是第一个温度值呢？lastTemp默认是0.0
      val prevTemp = lastTemp.value()

      // 将当前元素的温度值使用.update()方法对lastTemp进行赋值
      lastTemp.update(value.temperature)

      val curTimerTs = currentTimer.value()

      if (prevTemp == 0.0 || value.temperature < prevTemp) {
        // 温度是第一个温度值或者温度下降
        // 如果有报警事件存在，删除对应时间戳的定时事件
        ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
        // 清空状态变量
        currentTimer.clear()
      } else if (value.temperature > prevTemp && curTimerTs == 0) {
        // 温度上升且没有报警事件存在
        // ts是1s之后的时间戳
        val ts = ctx.timerService().currentProcessingTime() + 1000
        ctx.timerService().registerProcessingTimeTimer(ts)
        currentTimer.update(ts)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("传感器id为：" + ctx.getCurrentKey + " 的传感器温度值已经连续1s上升了！")
      currentTimer.clear()
    }
  }
}