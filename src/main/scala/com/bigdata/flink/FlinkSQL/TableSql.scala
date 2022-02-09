package com.bigdata.flink.FlinkSQL

/**
 * @Auther: tjk
 * @Date: 2020-06-25 14:32
 * @Description:
 */

object TableSql {
  def main(args: Array[String]): Unit = {
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
