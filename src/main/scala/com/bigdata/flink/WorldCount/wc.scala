package com.bigdata.flink.WorldCount

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Auther: tjk
  * @Date: 2020-03-02 21:53
  * @Description:
  */

object wc {
  def main(args: Array[String]): Unit = {


//     非流式 的用 ExecutionEnvironment, 离线的处理使用
//     流式   的用 StreamExecutionEnvironment，实时的处理使用
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val lines = env.readTextFile(args(0))

    val words = lines.flatMap(_.split("\\s+"))
    val wordsAndOne = words.map((_, 1))
    val grouped = wordsAndOne.groupBy(0)
    val sumed = grouped.sum(1)

    val save = sumed.writeAsText(args(1))

    env.execute("wc")

  }
}
