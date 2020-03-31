package com.bigdata.flink.Flink_hive

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
  * @Auther: tjk
  * @Date: 2020-03-28 15:54
  * @Description:
  */

object FlinkOnHive {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)


    val name            = "myhive"
    val defaultDatabase = "mydatabase"
    val hiveConfDir     = "/usr/local/hive/conf"
    val version         = "1.2.1" // or 2.3.4

    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
    tableEnv.registerCatalog("myhive", hive)

    tableEnv.sqlQuery(
      """
        |select * from
      """.stripMargin)


    env.execute()
  }
}
