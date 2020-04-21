package com.atguigu.utils

import java.util.Calendar
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import scala.collection.immutable
import scala.util.Random

class SensorSource extends RichParallelSourceFunction[SensorReading]{

  var running = true

  // 样板代码
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    // 高斯噪声
    var curFTemp: immutable.IndexedSeq[(String, Double)] = (1 to 10).map(i => {
      ("sensor_" + i, 65 + (rand.nextGaussian() * 20))
    })

    while(running) {

      curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 20)))

      val curTime: Long = Calendar.getInstance().getTimeInMillis

      curFTemp.foreach(t => sourceContext.collect(SensorReading(t._1, curTime, t._2)))

      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = running = false
}
