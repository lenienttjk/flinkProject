package com.bigdata.flink.FlinkTableApi

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}
/**
  * @Auther: tjk
  * @Date: 2020-03-31 13:18
  * @Description:
  */

object ScalaBatchWordCount {
  def main(args: Array[String]): Unit = {
    // 1.创建批处理环境
    val env =ExecutionEnvironment.getExecutionEnvironment

    val tableEnv = BatchTableEnvironment.create(env)

    val filePath = this.getClass.getClassLoader.getResource("word.txt").getPath

    tableEnv.connect(new FileSystem().path(filePath))
        .withFormat(new OldCsv().field("line",Types.STRING).lineDelimiter("\n"))
        .withSchema(new Schema().field("line",Types.STRING))
      // 注册成表资源 表名 ： fileSource
        .registerTableSource("fileSource")

    // 扫描注册的表,输出
    val resultTable = tableEnv.scan("fileSource")
      // 分组聚合
        .groupBy('line)
      // 查询
        .select('line,'line.count as 'count)


    // 插入到目标表 targetTable
    resultTable.insertInto("targetTable")


    // 从 taskManager收集回来 打印输出
    resultTable.collect().foreach(println)


  }
}
