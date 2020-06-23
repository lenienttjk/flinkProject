package com.bigdata.flink.Flink_table

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * @Auther: tjk
  * @Date: 2020-03-28 12:55
  * @Description:
  */

object flinksql {
  def main(args: Array[String]): Unit = {
    // environment configuration
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    // register Orders table in table environment
    // ...

    val csvData: DataSet[String] = env.readTextFile("E:\\Project\\flinkProject\\dataSource\\table.txt")

    // 使用 tableEnv 转换 输入数据为 table
    val topScore = tableEnv.fromDataSet(csvData)
    // 将 topScore 注册为一个表
    tableEnv.registerTable("topScore", topScore)




    //  扫描表
    val orders = tableEnv.scan("topScore") // schema (a, b, c, rowtime)

    val result = orders
      .groupBy('a)
      .select('a, 'b.count as 'cnt)

      .toDataSet[Row] // conversion to DataSet
      .print()


  }
}
