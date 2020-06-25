package com.bigdata.flink.Flink_source_sink.Flink_Sink

import java.util.Properties

import com.bigdata.flink.comment.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
  * 从 kafka 读取数据，使用 flink 处理
  * 将 flink 处理过的 数据 保存到 Kafka
  */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 如果不设，默认为本机核数的并行度，即读取数据安顺序一条条读取
    env.setParallelism(1)

    // 1、读取数据,从kafka读取

    //  kafka 配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "mini1:9092,mini2:9092,mini3:9092")
    properties.setProperty("group.id", "g1")
    properties.setProperty("key.serialization", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.serialization", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 2.从kafka 读取数据
    //  指定 topic ,kafka 配置
    // 输入 topic sensorInput
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String](
      "sensorInput",
      new SimpleStringSchema(),
      properties)
    )


    // 3、转换操作
    val dataStream = inputStream.map(data => {
      val daraArray = data.split(",")
      val sensorId = daraArray(0).trim
      val timeStamp = daraArray(1).trim.toLong
      val tempperture = daraArray(2).trim.toDouble

      // 转换为 String 类型，方便Flink  Sink 到 kafka。方便序列化
      SensorReading(sensorId, timeStamp, tempperture).toString
    })


    // 4、从文件读取数据，sink 到 kafka，作为kafka的生产者
    // 传入  servers,topic
    // 输出  topic sensorOutput
    // FlinkKafkaProducer011实现的是 两次提交  TwoPhaseCommitSinkFunction
    dataStream.addSink(new FlinkKafkaProducer011[String]("mini1:9092,mini2:9092,mini3:9092", "sensorOutput", new SimpleStringSchema()))
    dataStream.print()


    //    5 、启动流，不停止
    env.execute("KafkaSinkTest")

  }
}

/*
启动kafka 生产者
./bin/kafka-console-producer.sh --broker-list mini1:9092,mini2:9092,mini3:9092 --topic sensorInput


启动kafka 消费者
./bin/kafka-console-consumer.sh --bootstrap-server  mini1:9092,mini2:9092,mini3:9092 --topic sensorOutput


 */