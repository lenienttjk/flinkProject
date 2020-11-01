package com.bigdata.flink.FlinkSourceSink.Flink_DataSource

import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * 从文件读取数据
  */
object FlinkReadDataFromMySQL {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val dataStream =  env.addSource(new JdbcReader())
    // 读取mysql数据，获取dataStream后可以做逻辑处理，这里没有






    // 输出，并设置并行度为1
    dataStream.print("stream1:").setParallelism(1)



    //    启动流，不停止
    env.execute("readfile")
  }
}
