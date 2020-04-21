package com.atguigu

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction


object TableFunctionUDFExample {
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
        "hello#world",
        "atguigu#zuoyuan"
      )
      val table = tEnv.fromDataStream(stream, 's)
      val split = new Split("#")
      table
        // "hello#world"和"hello"join到一行
        .joinLateral(split('s) as ('word, 'length))
        .select('s, 'word, 'length)
        .toAppendStream[(String, String, Long)]
      //        .print()

      tEnv.registerFunction("split", new Split("#"))
      tEnv.createTemporaryView("t", table, 's)

      tEnv
        // T 是flink sql元组的语法
        .sqlQuery("SELECT s, word, length FROM t, LATERAL TABLE(split(s)) as T(word, length)")
        .toAppendStream[(String, String, Int)]
        .print()

      env.execute()
    }

    class Split(separator: String) extends TableFunction[(String, Int)] {
      def eval(str: String): Unit = {
        str.split(separator).foreach(x => collect((x, x.length)))
      }
    }
  }
