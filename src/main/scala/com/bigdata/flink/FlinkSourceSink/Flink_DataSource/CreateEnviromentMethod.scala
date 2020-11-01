package com.bigdata.flink.FlinkSourceSink.Flink_DataSource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


// enviroment  --> source --> transform  --> sink
object CreateEnviromentMethod {

  def main(args: Array[String]): Unit = {

    // 方式 1、创建flink 运行环境
    // getExecutionEnvironment 会获取当前运行的环境
    val env1 = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env1.setParallelism(1)

    // 方式二  本地执行
    // 最底层的创建运行环境的方式，指定并行度
    val env2 = StreamExecutionEnvironment.createLocalEnvironment(2)


    // 方式三  远程执行，用于 打包，集群运行
    // 定制执行的主机名，端口，执行的jar的路径
    val env3 = StreamExecutionEnvironment.createRemoteEnvironment("mini1",6123,"/root/worldcount.jar")



  }
}
