package com.bigdata.flink.FlinkTableApi

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}
import org.apache.flink.types.Row

/**
  * @Auther: tjk
  * @Date: 2020-03-31 14:02
  * @Description:
  */

object ScalaStreamWordCount {
  def main(args: Array[String]): Unit = {
    // 1.创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)

    val filePath = this.getClass.getClassLoader.getResource("dataSource/word.txt").getPath

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv().field("line", Types.STRING).lineDelimiter(" "))
      .withSchema(new Schema().field("line", Types.STRING))
      .registerTableSource("fileSource")


    val resultTable = tableEnv.scan("fileSource")
      .groupBy('line)
      .select('line, 'line.count as 'count)


    implicit val tpe: TypeInformation[Row] = Types.ROW(Types.STRING, Types.LONG)

    // 转化为Stream,输出
    resultTable.toRetractStream[Row].print()

    env.execute()
  }
}
