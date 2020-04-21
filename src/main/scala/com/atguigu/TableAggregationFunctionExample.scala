package com.atguigu

import com.atguigu.MinMaxTempExample.MyMinMax
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object TableAggregationFunctionExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.fromElements((1,-2),(1,2))
    val myAggFunc = new MyTableMinMax
    val table = tEnv.fromDataStream(stream, 'key, 'a)
    table.groupBy('key)
        .aggregate(myAggFunc('a) as ('x, 'y))
        .select('key,'x,'y)
        .toRetractStream[(Int,Int,Int)]
        .print()

//    val myAggFunc = new MyTableMinMax
//
//    val table = tEnv.fromDataStream(stream, 'key, 'a)
//
//    table.groupBy('key)
//      .aggregate(myAggFunc('a) as('x,'y))
//      .select('key,'x,'y)
//      .toRetractStream[(Int,Int,Int)]
//      .print()

    env.execute()
  }

  case class MyMinMaxAcc(var min: Int, var max: Int)

  class MyTableMinMax extends AggregateFunction[Row, MyMinMaxAcc] {
    override def getValue(acc: MyMinMaxAcc): Row = {
      Row.of(Integer.valueOf(acc.min), Integer.valueOf(acc.max))
  }

    override def createAccumulator(): MyMinMaxAcc = {
      MyMinMaxAcc(Int.MaxValue, Int.MinValue)
    }

    def accumulate(acc:MyMinMaxAcc, value:Int): Unit = {
      if (value < acc.min) {
        acc.min = value
      }

      if (value > acc.max) {
        acc.max = value
      }
    }

    def resetAccumulator(acc:MyMinMaxAcc): Unit = {
      acc.min = Int.MaxValue
      acc.max = Int.MinValue
    }

    override def getResultType: TypeInformation[Row] = {
      new RowTypeInfo(Types.INT, Types.INT)
    }
  }

//  class MyTableMinMax extends AggregateFunction[Row, MyMinMaxAcc] {
//    override def createAccumulator(): MyMinMaxAcc = MyMinMaxAcc(Int.MaxValue, Int.MinValue)
//
//    def accumulate(acc: MyMinMaxAcc, value: Int): Unit = {
//      if (value < acc.min) {
//        acc.min = value
//      }
//      if (value > acc.max) {
//        acc.max = value
//      }
//    }
//
//    def resetAccumulator(acc: MyMinMaxAcc): Unit = {
//      acc.min = Int.MaxValue
//      acc.max = Int.MinValue
//    }
//
//    override def getValue(accumulator: MyMinMaxAcc): Row = {
//      Row.of(Integer.valueOf(accumulator.min), Integer.valueOf(accumulator.max))
//    }
//
//    override def getResultType: TypeInformation[Row] = {
//      new RowTypeInfo(Types.INT, Types.INT)
//    }
//  }
}
