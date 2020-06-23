package com.bigdata.flink.Flink_State

import com.bigdata.flink.Flink_EventTime.TempIncreAlert
import com.bigdata.flink.Tranform_Operator.SensorReading
import org.apache.flink.api.common.functions.{RichFlatJoinFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @Auther: tjk
  * @Date: 2020-02-18 16:27
  * @Description:
  */

object StateTest {
  def main(args: Array[String]): Unit = {

    // 1、
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 导入 import org.apache.flink.streaming.api.TimeCharacteristic
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)


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

    val processedDataStream1 = dataStream.keyBy(_.id)
      .process(new TempIncreAlert())


    // 状态编程之方法1,2，3
    val processedDataStream2 = dataStream.keyBy(_.id)

      // 方法1
      //      .process(new TempChangeAlert1(10.0))

      // 方法2
      //      .flatMap(new TempChangeAlert2(10.0))

      // 方法3
      .flatMapWithState[(String, Double, Double), Double] {
      case (input: SensorReading, None) => (List.empty, Some(input.tempPerture))

      case (input: SensorReading, lastTemp: Some[Double]) =>
        val diff = (input.tempPerture - lastTemp.get).abs
        if (diff > 10.0) {
          (List((input.id, lastTemp.get, input.tempPerture)), Some(input.tempPerture))
        } else {
          // 也需要更新
          (List.empty, Some(input.tempPerture))
        }
    }



    // 5、打印输出
    dataStream.print("inputStream")
    processedDataStream1.print("processedDataStream")

    // 6、启动
    env.execute("WindowTest")

  }
}

//process 可以用状态编程
//

// 需求：检测2次温度之间的变化；不能超过一定的范围，超过则报警
// 例如 第一次 14，第二次24，则报警，限制条件：2次温差不能超过10
class TempChangeAlert1(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {


  // 1.定义一个状态，保存上一次的温度
  lazy val lastTempState: ValueState[Double] = getRuntimeContext
    // 状态名,类型  "lastTemp", classOf[Double]
    .getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  override def processElement(value: SensorReading, context: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, collector: Collector[(String, Double, Double)]): Unit = {
    // 获取上一次的温度
    val lastTempPerture = lastTempState.value()

    val diff = (value.tempPerture - lastTempPerture).abs
    if (diff > threshold) {
      collector.collect(value.id, lastTempPerture, value.tempPerture)
    }

    lastTempState.update(value.tempPerture)

  }
}


// 继承 Rich FlatMap Function 也可以实现状态编程
// 可以不用继承 ProcessFunction 大招，ProcessFunction 是通用的大招


// SensorReading, (String, Double, Double)   输入类型：SensorReading 输出类型：(String, Double, Double)
class TempChangeAlert2(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  private var lastTempState: ValueState[Double] = _


  override def open(parameters: Configuration): Unit = {
    //  初始化声明lastTempState 变量
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上一次的温度
    val lastTemp = lastTempState.value()

    val diff = (value.tempPerture - lastTemp).abs
    if (diff > threshold) {
      out.collect(value.id, lastTemp, value.tempPerture)
    }
    lastTempState.update(value.tempPerture)
  }


}

