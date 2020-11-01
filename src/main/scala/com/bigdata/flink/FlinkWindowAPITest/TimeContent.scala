package com.bigdata.flink.FlinkWindowAPITest

import org.apache.flink.api.scala._
import com.bigdata.flink.TranformOperator.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * flink 只有2种 time :Event Time ,Process Time
  * 时间语义，水位线watermark
  *
  * 1.设置Event Time
  * 2.水位线watermark 的传递，引入，设定  :
  *
  * 水位线作用：避免数据乱序带来计算不正确
  * 遇到一个时间戳到达了窗口关闭时间，不应该立刻触发窗口计算数据，
  * 而是等待一段时间，等迟到的数据到来再计算
  *
  * watermask 是一种衡量Event time 进展的机制，可以设定延迟触发
  * watermask 用来平衡延迟和结果的正确性。
  *
  *
  *
  *
  *
  */


object TimeContent {
  def main(args: Array[String]): Unit = {

    // 1、创建运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 在代码中设置Event Time。追加时间特征
    // 没有设置则 按照 ProcessingTime 处理


    // 2、
    val inputStream = env.readTextFile("E:\\Project\\flinkProject\\data.txt")

    // 3、转换操作
    val dataStream = inputStream.map(data => {
      val daraArray = data.split(",")
      val sensorId = daraArray(0).trim
      val timeStamp = daraArray(1).trim.toLong
      val tempperture = daraArray(2).trim.toDouble
      //  包装成样例类
      SensorReading(sensorId, timeStamp, tempperture)
    })



    // 4、输出，并设置并行度为1
    dataStream.print("stream:").setParallelism(1)




    //  5、 启动流，不停止
    env.execute("TimeContent")

  }

}
