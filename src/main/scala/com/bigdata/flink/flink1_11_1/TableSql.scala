package com.bigdata.flink.flink1_11_1

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Auther: tjk
  * @Date: 2020-06-25 14:32
  * @Description:
  */

object TableSql {
  def main(args: Array[String]): Unit = {

    import org.apache.flink.table.api.EnvironmentSettings
    import org.apache.flink.table.api.TableEnvironment
    val settings = EnvironmentSettings.newInstance.build
    val tEnv = TableEnvironment.create(settings)

    tEnv.executeSql(
      """
        |CREATE TABLE transactions (
        | account_id  BIGINT,
        | amount      BIGINT,
        |transaction_time TIMESTAMP(3),
        |ATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND
        |WITH (
        | 'connector' = 'kafka',
        |'topic'     = 'transactions',
        |'properties.bootstrap.servers' = 'kafka:9092',
        | 'format'    = 'csv'
        | )
        |
      """.stripMargin)


    tEnv.executeSql(
      """
        |CREATE TABLE user_behavior (
        |  user_id BIGINT,
        |  item_id BIGINT,
        |  category_id BIGINT,
        |  behavior STRING,
        |  ts TIMESTAMP(3)
        |) WITH (
        | 'connector' = 'kafka',
        | 'topic' = 'user_behavior',
        | 'properties.bootstrap.servers' = 'localhost:9092',
        | 'properties.group.id' = 'testGroup',
        | 'format' = 'json',
        | 'json.fail-on-missing-field' = 'false',
        | 'json.ignore-parse-errors' = 'true'
        |)
      """
        .stripMargin)


  }
}
