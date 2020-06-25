package com.bigdata.flink.Flink_source_sink.Flink_DataSource

import com.bigdata.flink.comment.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
  * 从集合读取数据
  */
object Sensor {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.fromCollection(
      List(
        SensorReading("sensor_1", 121414242, 34.1412342424),
        SensorReading("sensor_2", 121414524, 35.1412342424),
        SensorReading("sensor_3", 121443424, 36.1412342424),
        SensorReading("sensor_4", 121478424, 37.1412342424)
      )
    )
    // 输出，并设置并行度为1
    inputStream.print("stream1:").setParallelism(1)

    //    启动流，不停止
    env.execute("Sensor")
  }
}
