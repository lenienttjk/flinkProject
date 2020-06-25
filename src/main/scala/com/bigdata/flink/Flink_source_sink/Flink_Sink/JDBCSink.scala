package com.bigdata.flink.Flink_source_sink.Flink_Sink

import java.sql.{Connection, Driver, DriverManager, PreparedStatement}

import com.bigdata.flink.comment.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
  * 将 flink 处理过的 数据 保存到 Redis
  */
object JDBCSink {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 如果不设，默认为本机核数的并行度，即读取数据安顺序一条条读取
    env.setParallelism(1)

    // 1、读取数据,从kafka读取

    val inputStream = env.readTextFile("E:\\Project\\flinkProject\\data.txt")


    // 3、转换操作
    val dataStream = inputStream.map(data => {
      val daraArray = data.split(",")
      val sensorId = daraArray(0).trim
      val timeStamp = daraArray(1).trim.toLong
      val tempperture = daraArray(2).trim.toDouble

      //  包装成样例类
      SensorReading(sensorId, timeStamp, tempperture)
    })


    inputStream.print("stream")

    // 4、从文件读取数据，sink 到 Mysql
    dataStream.addSink(new MyJdbcSink())



    //    5 、启动流，不停止
    env.execute("JDBCSink")

  }
}

//  继承 RedisMapper
class MyJdbcSink() extends RichSinkFunction[SensorReading] {
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  // 1、打开连接
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val driver = "com.mysql.jdbc.Driver"
    Class.forName(driver)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3307/test", "root", "123456")
    insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor,temp) VALUES(?,?)")
    updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")

  }


  //2、 调用连接，执行 sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {

    // 2.1 先执行更新语句，没有更新，则插入语句
    updateStmt.setDouble(1, value.tempPerture)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    //2.2  如果更新的条数为0，则插入,注意setString() 中的数字，代表第几个参数
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.tempPerture)
      insertStmt.execute()
    }
  }


  // 3、关闭连接，有先后
  override def close(): Unit = {
    // 先关插入，再关更新，最后关闭数据库连接
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }

}
