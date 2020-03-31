package com.bigdata.flink.Flink_EventTime

import com.bigdata.flink.Tranform_Operator.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @Auther: tjk
  * @Date: 2020-02-17 20:43
  * @Description:
  */

object SlideOutputTest {
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

    val processedDataStream = dataStream
      .process(new FreezingAlert())




    // 5、打印输出
    dataStream.print("inputData")
    processedDataStream.print("processData")
    // 输出侧输出流
    processedDataStream.getSideOutput(new OutputTag[String]("freezing alert")).print()

    // 6、启动
    env.execute("SlideOutputTest")

  }
}

// 需求： 华氏温度 32F 摄氏温度0度，冰点报警
//  第一个 是 输入数据类型，第二个是 主输出流的类型
class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading] {

  lazy val alertOutput: OutputTag[String] = new OutputTag[String]("freezing alert")

  override def processElement(value: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.tempPerture < 32.0) {
      context.output(alertOutput, "Freezing Alert for " + value.id)
    } else {
      out.collect(value)
    }
  }
}
