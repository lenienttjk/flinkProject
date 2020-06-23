package com.bigdata.flink.Flink_DataSource

import java.util.Properties
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer011}


/**
  * 从文件读取数据
  */
object FlinkReadDataFromKafka {
  def main(args: Array[String]): Unit = {

    // 1.创建运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    //  kafka 配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "mini1:9092,mini2:9092,mini3:9092")
    properties.setProperty("group.id", "g1")
    properties.setProperty("key.serialization", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.serialization", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 3.从kafka 读取数据
    //  指定 topic ,kafka 配置
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))




    // 对数据不做复杂处理，输出，并设置并行度为1
    inputStream.print("stream1:").setParallelism(1)
    //    启动流，不停止
    env.execute("flinkReadDataFromKafka")
  }
}

/*
启动kafka 生产者
./bin/kafka-console-producer.sh --broker-list mini1:9092,mini2:9092,mini3:9092 --topic sensor

// 一行行输入数据
sensor_1,121414242,34.1412342424
sensor_2,121414524,35.1412342424

 */
