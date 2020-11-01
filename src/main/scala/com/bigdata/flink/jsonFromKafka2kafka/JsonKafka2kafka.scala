package com.bigdata.flink.jsonFromKafka2kafka

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema



object JsonKafka2kafka {
  def main(args: Array[String]): Unit = {

    // 1.创建运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 非常关键，一定要设置启动检查点！！
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //  kafka 配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "mini1:9092,mini2:9092,mini3:9092")
    properties.setProperty("group.id", "g1")
    properties.setProperty("key.serialization", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.serialization", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 3.从kafka 读取数据
    //  指定 topic ,kafka 配置
    //  new SimpleStringSchema() 直接返回的是kafka发送过来的数据
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("maxwell", new SimpleStringSchema(), properties))



    val inputStream1 = env.addSource(new FlinkKafkaConsumer011("maxwell",new JSONKeyValueDeserializationSchema(true), properties))







    // 转换为字符串
    val str = inputStream.toString

    // 解析 json
    import com.alibaba.fastjson.{JSON, JSONObject}

    // 转化为json
    val jsonObj = JSON.parseObject(str)

    jsonObj.getString("data")










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
