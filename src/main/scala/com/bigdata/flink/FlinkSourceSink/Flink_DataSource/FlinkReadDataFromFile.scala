package com.bigdata.flink.FlinkSourceSink.Flink_DataSource

// 注意：必须导入这两行，否则报隐式没有
import com.alibaba.fastjson
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011


/**
  * 从文件读取数据
  */
object FlinkReadDataFromFile {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取windows本地文件
    //    val inputStream = env.readTextFile("E:\\Project\\flinkProject\\data.txt")
    val inputStream: DataStream[String] = env.readTextFile("E:\\Project\\flinkProject\\dataSource\\test.json")

    // 读取linux 的本地文件
    //    val inputStream1 = env.readTextFile("file:///root/wc.txt")

    //
    // 将kafka的数据转换为json
    val jsonStream = inputStream.map(t => JSON.parseObject(t))



    val ds: DataStream[(String, String, String, String, String, String, String, String, String, String, String)] = jsonStream.map { x =>

      val db = x.getString("database")
      val tb = x.getString("table")
      val tp = x.getString("type")

      val ts: String = x.getString("ts")

      val data = x.getJSONObject("data")
      val user_id = data.getString("user_id")
      val username = data.getString("username")
      val password = data.getString("password")
      val salt = data.getString("salt")
      val email = data.getString("email")
      val mobile = data.getString("mobile")
      val status = data.getString("status")
      (db, tb, tp, ts, user_id, username, password, salt, email, mobile, status)
    }

    ds.print("ds:====").setParallelism(1)

// 可以在Flink SQL里 调用 DATE_FORMAT(ts,"yyyy-MM-dd HH:mm:ss")
    // sink
    // FlinkKafkaProducer011实现的是 两次提交  TwoPhaseCommitSinkFunction
//    ds.addSink(new FlinkKafkaProducer011[(String, String, String, String, String, String, String, String, String, String, String)]("mini1:9092,mini2:9092,mini3:9092", "sensorOutput", new SimpleStringSchema()))



    // 输出，并设置并行度为1
    //    inputStream.print("stream1:").setParallelism(1)


    //    启动流，不停止
    env.execute("readfile")
  }
}
