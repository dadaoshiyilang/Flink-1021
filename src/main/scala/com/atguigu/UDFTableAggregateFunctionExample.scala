package com.atguigu

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import java.lang.{Integer => JInteger}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import java.lang.{Iterable => JIterable}
import org.apache.flink.util.Collector

object UDFTableAggregateFunctionExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    val stream = env.fromElements(
      (1, -1),
      (1, 2),
      (1, 0),
      (1, 5),
      (1, 4)
    )
    val top2 = new Top2
    val table = tEnv.fromDataStream(stream, 'key, 'a)
    table
      .groupBy('key)
      .flatAggregate(top2('a) as ('v, 'rank))
      .select('key, 'v, 'rank)
      .toRetractStream[(Int, Int, Int)]
      .print()

    env.execute()
  }

  class Top2Accum {
    var first: JInteger = _
    var second: JInteger = _
  }

  class Top2 extends TableAggregateFunction[JTuple2[JInteger, JInteger], Top2Accum] {
    override def createAccumulator(): Top2Accum = {
      val acc = new Top2Accum
      acc.first = Int.MinValue
      acc.second = Int.MinValue
      acc
    }
    def accumulate(acc: Top2Accum, v: Int): Unit = {
      if (v > acc.first) {
        acc.second = acc.first
        acc.first = v
      } else if (v > acc.second) {
        acc.second = v
      }
    }
    def merge(acc: Top2Accum, its: JIterable[Top2Accum]): Unit = {
      val iter = its.iterator()
      while (iter.hasNext) {
        val top2 = iter.next()
        accumulate(acc, top2.first)
        accumulate(acc, top2.second)
      }
    }
    def emitValue(acc: Top2Accum, out: Collector[JTuple2[JInteger, JInteger]]): Unit = {
      if (acc.first != Int.MinValue) {
        out.collect(JTuple2.of(acc.first, 1))
      }
      if (acc.second != Int.MinValue) {
        out.collect(JTuple2.of(acc.second, 2))
      }
    }
  }
}
