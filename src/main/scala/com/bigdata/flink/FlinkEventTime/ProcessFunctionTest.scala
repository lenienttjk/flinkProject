package com.bigdata.flink.FlinkEventTime

import com.bigdata.flink.TranformOperator.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @Auther: tjk
  * @Date: 2020-02-17 20:43
  * @Description:
  */

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {

    // 1、
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 导入 import org.apache.flink.streaming.api.TimeCharacteristic
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // nc -lk
    val inputStream = env.socketTextStream("mini1", 8888)


    // 3、转换操作
    val dataStream = inputStream.map(data => {
      val daraArray = data.split(",")
      val sensorId = daraArray(0).trim
      val timeStamp = daraArray(1).trim.toLong
      val tempperture = daraArray(2).trim.toDouble
      //  包装成样例类
      SensorReading(sensorId, timeStamp, tempperture)
    })

      // 方法2： 增加事件时间
      .assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = {
          t.timeStamp * 1000
        }
      }
    )

    val processedDataStream = dataStream.keyBy(_.id)
      .process(new TempIncreAlert())




    // 5、打印输出
    dataStream.print("inputStream")
    processedDataStream.print("processedDataStream")

    // 6、启动
    env.execute("WindowTest")

  }
}

// 需求：检测2次的温度之间对比，第二次温度是否比上一次上升
class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String] {

  // 1.定义一个状态，保存上一次的温度
  lazy val lastTemp: ValueState[Double] = getRuntimeContext
    // 状态名,类型  "lastTemp", classOf[Double]
    .getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))


  //  2.定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext
    // 状态名,类型  "lastTemp", classOf[Double]
    .getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))


  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {


    //   1. 取出上一次的温度
    val preTemp = lastTemp.value()
    // 更新温度
    lastTemp.update(value.tempPerture)

    // 2.取出上一次的时间戳
    val curTimerTs: Long = currentTimer.value()

    // 3.温度上升，并且没有设置过定时器(curTimerTs 为 0)，注册定时器
    // 当前温度比上一温度大，
    if (value.tempPerture > preTemp && curTimerTs == 0) {
      // 3.1 注册定时器
      val timerTs = ctx.timerService().currentProcessingTime() + 1000L
      ctx.timerService().registerProcessingTimeTimer(timerTs)

    } else if (preTemp > value.tempPerture || preTemp != 0) {
      // 3.2 删除定时器,清空状态
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear()
    }

  }

  // 调用方法 timestamp  ctx  out
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 直接输出报警信息
    out.collect("sensor_" + ctx.getCurrentKey + " 温度比上一次上升")
    currentTimer.clear()
  }
}
