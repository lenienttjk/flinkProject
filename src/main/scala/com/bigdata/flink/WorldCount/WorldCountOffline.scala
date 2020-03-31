package com.bigdata.flink.WorldCount

import org.apache.flink.api.scala._

object WorldCountOffline {
  def main(args: Array[String]) {
    //初始化环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //分割字符串、汇总tuple、按照key进行分组、统计分组后word个数
    val countStream = env.readTextFile("E:\\Project\\flinkProject\\dataSource\\log.txt")
      .flatMap(_.toLowerCase.split("\\s+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    //打印
    countStream.print()


  }
}
