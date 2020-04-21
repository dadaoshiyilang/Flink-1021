package com.atguigu

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WatermarkExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
     env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream =env.socketTextStream("hadoop102", 9999, '\n').map(r => {
      val arr: Array[String] = r.split(" ")
      (arr(0), arr(1).toLong * 1000)
      // 必须在source后面，或者说必须在keyby之前，抽取时间戳和产生水位线
      // source并行度为1
      // Flink默认每隔200ms(机器时间)向数据流中插入一次Watermark

    }).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(10)) {
        override def extractTimestamp(t: (String, Long)): Long = t._2
      }
    ).keyBy(_._1).timeWindow(Time.seconds(5)).process(new PW)

    stream.print()
    env.execute()
  }

//  class PW extends ProcessWindowFunction[(String,Long),String,Long,TimeWindow] {
//    override def process(key: Long, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
//      out.collect("窗口结束时间为"+ context.window.getEnd + "的窗口闭合了！"+"元素数量是："+elements.size+"个")
//    }
//  }

  class PW extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect("窗口结束时间为 " + context.window.getEnd + " 的窗口闭合了！" + "元素数量是：" + elements.size + " 个")
    }
  }
}
