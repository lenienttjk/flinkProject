package com.bigdata.flink.FlinkSourceSink.flinkdatasource

import java.util.Properties

import com.alibaba.fastjson
import com.alibaba.fastjson.JSON
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer011}

import scala.util.parsing.json.JSONObject


/**
  * 从文件读取数据
  */
object FlinkReadDataFromKafka {
  def main(args: Array[String]): Unit = {

    // 1.创建运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)


    //  kafka 配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "mini1:9092,mini2:9092,mini3:9092")
    properties.setProperty("group.id", "g1")
    properties.setProperty("key.serialization", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.serialization", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 3.从kafka 读取数据
    //  指定 topic ,kafka 配置
    val inputKafkaStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    // 将kafka的数据转换为json
    val jsonStream = inputKafkaStream.map(t => JSON.parseObject(t))

    //
    val ds: DataStream[(String, String, String, String, fastjson.JSONObject)] = jsonStream.map { x =>

      val db = x.getString("database")
      val tb = x.getString("table")
      val tp = x.getString("type")
      val ts = x.getString("ts")
      val data = x.getJSONObject("data")
      val user_id = data.getString("user_id")
      val username = data.getString("username")
      val password = data.getString("password")
      val salt = data.getString("salt")
      val email = data.getString("email")
      val mobile = data.getString("mobile")
      val status = data.getString("status")
      (db, tb, tp, ts, data)
    }

    ds.print("ds:====").setParallelism(1)



    // 对数据不做复杂处理，输出，并设置并行度为1
//    jsonStream.print("stream1:").setParallelism(1)
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
