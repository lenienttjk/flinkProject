package com.bigdata.flink.Flink_source_sink.Flink_Sink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}

/**
  * @Auther: tjk
  * @Date: 2020-06-24 22:45
  * @Description:
  */

object Kafka2kafka {
  def main(args: Array[String]): Unit = {
    // 1.创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // tableEnv
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 1、tableAPI 连接Kafka source
    tableEnv.connect(
      new Kafka()
        .version("0.11")
        .topic("sensor_source")
        .property("bootstrap.servers", "mini1:9092,mini2:9092,mini3:9092")
        .property("zookeeper.connect", "mini1:2181,mini2:2181,mini3:2181")
    )
      .withFormat(new Json())
      .withSchema(
        new Schema()
          .field("id", DataTypes.STRING())
          .field("temperture", DataTypes.DOUBLE())
          .field("timestamp", DataTypes.BIGINT())

      )
      .createTemporaryTable("kafkaInputTable")


    ///2、转换操作
    val sensorTable: Table = tableEnv.from("kafkaInputTable")

    val resultTable: Table = sensorTable.select('id, 'temperture).filter('id == "sensor_1")

    val aggResultTable: Table = sensorTable.groupBy('id)
      .select('id, 'id.count as 'count)


    // 3、输出到kafka
    tableEnv.connect(
      new Kafka()
        .version("0.11")
        .topic("sensor_sink")
        .property("bootstrap.servers", "mini1:9092,mini2:9092,mini3:9092")
        .property("zookeeper.connect", "mini1:2181,mini2:2181,mini3:2181")
    )
      .withFormat(new Json())
      .withSchema(
        new Schema()
          .field("id", DataTypes.STRING())
          .field("temperture", DataTypes.DOUBLE())
          .field("timestamp", DataTypes.BIGINT())

      )
      .createTemporaryTable("kafkaOutputTable")


    env.execute()
  }
}
