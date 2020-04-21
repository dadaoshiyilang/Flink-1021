package com.atguigu

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTImeoutWithoutCEP {

  case class OrderEvent(orderId: String, eventType: String, eventTime: String)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderEventStream = env
      .fromCollection(List(
        OrderEvent("1", "create", "1558430842"),
        OrderEvent("2", "create", "1558430843"),
        OrderEvent("2", "pay", "1558430844"),
        OrderEvent("3", "pay", "1558430942"),
        OrderEvent("4", "pay", "1558430943")
      ))
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
    val orders = orderEventStream
      .keyBy(_.orderId)
      .process(new OrderProcessFunction)
    orders.print()
    env.execute()

  }

  class  OrderProcessFunction extends  KeyedProcessFunction[String, OrderEvent, OrderEvent] {

    lazy val orderState = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("orderState", classOf[OrderEvent])
    )
    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[String, OrderEvent, OrderEvent]#Context, out: Collector[OrderEvent]): Unit = {

      if (value.eventType == "create") {
        if (orderState.value() == null) {
          orderState.update(value)
        }
      } else {
        orderState.update(value)
      }
      ctx.timerService().registerEventTimeTimer(value.eventTime.toLong * 1000 + 5 * 1000)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, OrderEvent, OrderEvent]#OnTimerContext, out: Collector[OrderEvent]): Unit = {
      val state = orderState.value()
      if (state != null && (state.eventType == "create")) {
        out.collect(state)
      }
      orderState.clear()
    }
  }
}
