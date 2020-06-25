package com.bigdata.flink.flink_kafka

import org.apache.flink.streaming.api.scala._

object Flink_readfile {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置全局并行度
    env.setParallelism(1)

    case class sensor(pw: String, phone: String, num: Double)
    // 数据格式 ad%3*@ikDJFDD&#j1 ,13423781579,131
    //          用户密码，手机号，下单笔数
    val streamData = env.readTextFile("E:\\Project\\flinkProject\\dataSource\\log.txt")


    val stream: DataStream[sensor] = streamData.map(t => {
      val arr = t.split(",")

      val pw = arr(0).trim
      val phone = arr(1)
      val order_num = arr(2).toDouble

      sensor(arr(0).trim, arr(1), arr(2).toDouble)
    })
      .keyBy(0)
      .sum(2)

    // 设置  print()的并行度 setParallelism(n)
    //    stream .print().setParallelism(1)
    stream.print()


    // 启动
    env.execute("Flink_readfile")
  }
}

/*
最终结果
sensor(ad%3*@ikDJFDD&#j1,13423781579,131.0)
sensor(ad%3*@ikDJFDD&#j5,13423781575,157.0)
sensor(ad%3*@ikDJFDD&#j3,13423781575,157.0)
sensor(ad%3*@ikDJFDD&#j4,13423781576,156.0)
sensor(ad%3*@ikDJFDD&#j5,13423781575,314.0)
sensor(ad%3*@ikDJFDD&#j4,13423781576,307.0)
sensor(ad%3*@ikDJFDD&#j2,13423781579,151.0)
sensor(ad%3*@ikDJFDD&#j4,13423781576,459.0)
sensor(ad%3*@ikDJFDD&#j1,13423781579,282.0)
sensor(ad%3*@ikDJFDD&#j4,13423781576,613.0)
 */