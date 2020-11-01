package com.bigdata.flink.FlinkWindowAPITest

import com.bigdata.flink.TranformOperator.SensorReading
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark

object EventTimeTest {
  def main(args: Array[String]): Unit = {

  }
}

// 自定义一个周期性的时间戳 抽取
// 继承 AssignerWithPeriodicWatermarks

class PeriodictAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {

  // 延迟1分钟触发窗口计算
  val bound: Long = 60 * 1000
  // 观察到的最大时间戳
  var maxTs: Long = Long.MinValue


  // Watermark 需要保证只涨不跌
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    maxTs = maxTs.max(t.timeStamp)
    t.timeStamp
  }
}


// 继承 AssignerWithPunctuatedWatermarks
class PunctuateAssigner extends AssignerWithPunctuatedWatermarks[SensorReading] {

  // 延迟1分钟触发窗口计算，1分钟60秒。1秒等于1000毫秒。计算机的默认的单位是毫秒
  val bound: Long = 60 * 1000


  override def checkAndGetNextWatermark(t: SensorReading, extractedTs: Long): Watermark = {
    if (t.id == "sensor_1") {
      new Watermark(extractedTs - bound)
    } else {
      null
    }
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    t.timeStamp

  }
}

