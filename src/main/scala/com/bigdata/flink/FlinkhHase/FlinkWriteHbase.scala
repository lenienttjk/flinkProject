package com.bigdata.flink.FlinkhHase

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Auther: tjk
  * @Date: 2020-08-23 21:56
  * @Description:
  */

object FlinkWriteHbase {
  def main(args: Array[String]): Unit = {
    // 1.创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    env.execute()
  }
}
