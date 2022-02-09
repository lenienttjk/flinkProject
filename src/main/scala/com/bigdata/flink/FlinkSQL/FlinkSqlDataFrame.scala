package com.bigdata.flink.FlinkSQL

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.util.Properties


/**
  * @Auther: tjk
  * @Date: 2020-06-25 22:34
  * @Description:
  */

object FlinkSqlDataFrame {
  def main(args: Array[String]): Unit = {
    // 1.创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)


    //  kafka 配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "mini1:9092,mini2:9092,mini3:9092")
    properties.setProperty("group.id", "g1")
    properties.setProperty("key.serialization", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.serialization", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")


    /*  // 3.从kafka 读取数据
      //  指定 topic ,kafka 配置
      val ds:DataStream[(Long,String,String)] = env.addSource()

      // SQL query with an inlined (unregistered) table
      val table = ds.toTable(tableEnv, 'user, 'product, 'amount)
      val result = tableEnv.sqlQuery(
        s"SELECT SUM(amount) FROM $table WHERE product LIKE '%Rubber%'")

      // SQL query with a registered table
      // register the DataStream under the name "Orders"
      tableEnv.createTemporaryView("Orders", ds, 'user, 'product, 'amount)
      // run a SQL query on the Table and retrieve the result as a new Table
      val result2 = tableEnv.sqlQuery(
        "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")

      // SQL update with a registered table
      // create and register a TableSink
      val schema = new Schema()
        .field("product", DataTypes.STRING())
        .field("amount", DataTypes.INT())

      tableEnv.connect(new FileSystem("/path/to/file"))
        .withFormat(...)
      .withSchema(schema)
        .createTemporaryTable("RubberOrders")*/

    // run a SQL update query on the Table and emit the result to the TableSink
    tableEnv.sqlUpdate(
      "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")


    env.execute()
  }
}
