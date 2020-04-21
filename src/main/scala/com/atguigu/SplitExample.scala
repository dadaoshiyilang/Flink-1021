package com.atguigu

import org.apache.flink.streaming.api.scala._

object SplitExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[(Int, String)] = env.fromElements((1001,"1001"),(999,"999"),(1000, "1000"))

    val splitted: SplitStream[(Int, String)] = stream.split(t => if (t._1 > 1000) Seq("large") else Seq("small"))

    val largeDStream: DataStream[(Int, String)] = splitted.select("large")

    largeDStream.print()

    // round-robin(rebalance)
    // rescale
    // broadcast:将输入流的所有数据复制发送到下游算子中

    env.execute()
  }

}
