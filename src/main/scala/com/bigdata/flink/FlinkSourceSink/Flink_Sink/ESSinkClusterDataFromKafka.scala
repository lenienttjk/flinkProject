package com.bigdata.flink.FlinkSourceSink.Flink_Sink


import java.util.Properties

import com.bigdata.flink.TranformOperator.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests


/**
  *
  * 将 flink 处理过的 数据 保存到 Elastic Search
  */
object ESSinkClusterDataFromKafka {
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
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensorInput", new SimpleStringSchema(), properties))

    //   2、 从文件读取
    //    val inputStream = env.readTextFile("E:\\Project\\flinkProject\\data.txt")

    // 3、转换操作
    val dataStream = inputStream.map(data => {
      val daraArray = data.split(",")
      val sensorId = daraArray(0).trim
      val timeStamp = daraArray(1).trim.toLong
      val tempperture = daraArray(2).trim.toDouble

      //  包装成样例类
      SensorReading(sensorId, timeStamp, tempperture)
    })


    // 4、从文件读取数据，sink 到 ES
    // ES 配置 方法1
    //    val httpHosts = new util.ArrayList[HttpHost]()
    //    httpHosts.add(new HttpHost("mini1", 9200))
    //    httpHosts.add(new HttpHost("mini2", 9200))
    //    httpHosts.add(new HttpHost("mini3", 9200))


    // ES 配置 方法2  List 默认是scala的List,需要隐式转换
    //      import scala.collection.JavaConversions._
    val httpHosts: List[HttpHost] = List(
      new HttpHost("mini1", 9200),
      new HttpHost("mini1", 9200),
      new HttpHost("mini1", 9200)
    )

    import scala.collection.JavaConversions._

    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {

        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data " + t)

          // 1. 创建一个map 包装 jsonObjet
          val json = new java.util.HashMap[String, String]()
          json.put("sensor_id", t.id)
          json.put("temperature", t.tempPerture.toString)
          json.put("timeStamp", t.timeStamp.toString)

          // 2.创建index 索引，为 sensor
          val indexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingData")
            .source(json)

          // 3.利用index 发送请求
          requestIndexer.add(indexRequest)

          println("saved successfully")
        }
      })

    // 4、sink 到ES
    dataStream.addSink(esSinkBuilder.build())



    //    5 、启动流，不停止
    env.execute("ESSinkTest")

  }
}


/*
启动kafka 生产者
./bin/kafka-console-producer.sh --broker-list mini1:9092,mini2:9092,mini3:9092 --topic sensorInput


 启动 ES 集群
 使用es用户
 cd /usr/local/Elasticsearch-6.5.0
   ./bin/elasticsearch


 */