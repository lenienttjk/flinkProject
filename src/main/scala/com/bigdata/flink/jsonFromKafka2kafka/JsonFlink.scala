package com.bigdata.flink.jsonFromKafka2kafka

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema


/**
  * @Auther: tjk
  * @Date: 2020-06-23 23:06
  * @Description:
  */

object JsonFlink {
  def main(args: Array[String]): Unit = {
    // 1.创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    //配置kafka信息
    val props = new Properties()
    props.setProperty("bootstrap.servers", "mini1:9092,mini2:9092,mini3:9092")
    props.setProperty("zookeeper.connect", "mini1:2181,mini2:2181,mini3:2181")
    props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("group.id", "test")
    props.setProperty("auto.offset.reset", "latest")
    //读取通过maxwell监控过来的数据并且变成datastream
    val sourcestream: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String]("Flink_Consumer", new SimpleStringSchema(), props)

    var env_source: DataStream[String] = env.addSource(sourcestream).map(source => {
      val maxwell_json: JSONObject = JSON.parseObject(source.toString)
      if (maxwell_json.getString("database") == "order_analysis" && (maxwell_json.getString("table") == "dm_cateNamesPrefer")) {
        maxwell_json.getString("data")
      }
      else
        null
    })


    //写入测试环境
    val KAFKA_BROKER_LIST = "mini1:9092,mini2:9092,mini3:9092"


    val kafka_sink_data = new FlinkKafkaProducer011[String](KAFKA_BROKER_LIST, "Flink_sink", new SimpleStringSchema())

    env_source.addSink(kafka_sink_data).name("jzb_user_cateNamesPrefer")


    env.execute("db_")

    env.execute()
  }
}
