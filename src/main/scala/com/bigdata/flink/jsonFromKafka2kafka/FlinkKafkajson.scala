package com.bigdata.flink.jsonFromKafka2kafka

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object FlinkKafkajson {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取windows本地文件
    val inputStream = env.readTextFile("E:\\Project\\flinkProject\\src\\main\\scala\\com\\bigdata\\flink\\json_from_kafka2kafka\\test.json")


   inputStream.print("")

    //    启动流，不停止
    env.execute("readfile")
  }
}


