package com.bigdata.flink.Flink_DataSource

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
  * 从文件读取数据
  */
object FlinkReadDataFromFile {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取windows本地文件
    val inputStream = env.readTextFile("E:\\Project\\flinkProject\\data.txt")

    // 读取linux 的本地文件
    val inputStream1 = env.readTextFile("file:///root/wc.txt")

    // 输出，并设置并行度为1
    inputStream.print("stream1:").setParallelism(1)

    //    启动流，不停止
    env.execute("readfile")
  }
}
