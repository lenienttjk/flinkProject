package com.bigdata.flink.FlinkWindowAPITest

import com.bigdata.flink.TranformOperator.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
//
/**
  *
  * 滚动窗口   滚动时间窗口  timeWindow(Time.seconds(15))
  * 滑动窗口   滑动时间窗口  timeWindow(Time.seconds(15)，Time.second(5))
  * 会话窗口                 .window(EventTimesSessionwindows.withGap(Time.minutes(5)))
  * 全局窗口
  *
  * 窗口函数
  * 1 增量聚合函数
  * 2 全窗口函数
  *
  * .triger() 触发器
  * .evitor() 移除器
  * .allowedLateness()   允许处理迟到的数据
  * .sideOutputLateData()  将迟迟来到的数据放到侧输出流
  * .getSideOutput()   侧输出流
  *
  */
object WindowTest {
  def main(args: Array[String]): Unit = {

    // 1、
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 导入 import org.apache.flink.streaming.api.TimeCharacteristic
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 周期性生成 watermark 默认时间为 200 毫秒
    env.getConfig.setAutoWatermarkInterval(100L)

    //    val inputStream = env.readTextFile("E:\\Project\\flinkProject\\data.txt")

    // nc -lk  由于一下子处理完，所以 从文件读取， 时间不够 窗口时间
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

      // 方法1：简单实现
      //      .assignAscendingTimestamps(_.timeStamp * 1000)

      // 方法2： 增加事件时间
      .assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = {
          t.timeStamp * 1000
        }
      }
    )


    // 4. 滚动 窗口操作
    // 统计15秒内的最小温度，每隔5秒输出一次
    val minTempPerWindowStream = dataStream
      .map(data => (data.id, data.tempPerture))
      .keyBy(_._1)
//      .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5)))
      //  导入 import org.apache.flink.streaming.api.windowing.time.Time
      .timeWindow(Time.seconds(5))

      // reduce 做增量聚合
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    // 5、打印输出
    minTempPerWindowStream.print("min tempperture")
    dataStream.print("inputStream")


    dataStream.keyBy(_.id)
        .process( new MyProcess())


    // 6、启动
    env.execute("WindowTest")
  }
}

class MyProcess() extends KeyedProcessFunction[String,SensorReading,String]{

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {

    // 可以传入，可以注册 context.timerService().registerEventTimeTimer(2000)
  context.timerService().registerEventTimeTimer(2000)

  }
}
