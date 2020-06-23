package com.bigdata.flink.json_from_kafka2kafka

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Auther: tjk
  * @Date: 2020-06-23 23:06
  * @Description:
  */

object json_flink {
  def main(args: Array[String]): Unit = {
    // 1.创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    env.execute()
  }
}
