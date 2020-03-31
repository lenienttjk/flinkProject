package com.bigdata.flink.WorldCount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object WorldCountForSocket {
  def main(args: Array[String]): Unit = {

    //    定义  socket port
    val port: Int = ParameterTool.fromArgs(args).getInt("port")
    val host: String = ParameterTool.fromArgs(args).get("host")

    // flink 可视化界面
    //  --host mini1 --port 8888

    // 获取运行环境
    val evn: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    连接 socket获取数据
    val text = evn.socketTextStream(host, port, '\n')

    //    解析数据，分组，窗口，聚合sum
    val countStream = text.flatMap {
      _.toLowerCase.split("\\s+") filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .keyBy(0)
      .sum(1)


    //输出
    countStream.print()

    evn.execute("Socket Window WordCount")

  }
}

/*

打包成jar包，提交到flink 集群运行命令
./bin/flink run \
-c com.bigdata.flinkWC.WorldCountForSocket \
/root/wc.jar \
--host mini1 \
--port 8000

 */

