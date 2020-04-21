package com.atguigu

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WordCount {

  case class WordWithCount(word:String, count:Long)

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val text: DataStream[String] = env.socketTextStream("hadoop102", 9999, '\n')

    val windowCounts: DataStream[WordWithCount] = text.flatMap(w => w.split("\\s"))
      .map(w => WordWithCount(w, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")
    windowCounts.print().setParallelism(1)
    env.execute()
  }
}
