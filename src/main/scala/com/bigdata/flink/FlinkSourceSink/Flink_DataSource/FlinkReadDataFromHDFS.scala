package com.bigdata.flink.FlinkSourceSink.Flink_DataSource

import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._


/**
  * 从文件读取数据
  */
object FlinkReadDataFromHDFS {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // 读取HDFS文件
    val inputStream = env.readTextFile("hdfs://mini1:9000/input/1.txt")

    // 直接输出，并设置并行度为1
    inputStream.print("stream1:").setParallelism(1)

    //    启动流，不停止
    env.execute("readfile")
  }
}
