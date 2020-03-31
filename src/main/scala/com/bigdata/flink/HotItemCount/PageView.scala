package com.bigdata.flink.HotItemCount

import com.bigdata.flink.HotItemCount.HotItems.{CountAgg, TopNHotItems, WindowResultFunction}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Auther: tjk
  * @Date: 2020-02-22 12:06
  * @Description:
  */

object PageView {
  def main(args: Array[String]): Unit = {
    // 创建一个env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 显式地定义Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream = env
      .readTextFile("E:\\Project\\flinkProject\\dataSource\\UserBehavior.csv")
      .map(line => {
        val linearray = line.split(",")
        UserBehavior(linearray(0).trim.toLong, linearray(1).trim.toLong, linearray(2).trim.toInt, linearray(3).trim, linearray(4).trim.toLong)
      })

      .assignAscendingTimestamps(_.timestamp * 1000L)
      // 只统计 页面统计
      .filter(_.behavior == "pv")
      .map(data => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    dataStream.print(" pv Count")



    // 调用execute执行任务
    env.execute("Page View job")

  }
}
