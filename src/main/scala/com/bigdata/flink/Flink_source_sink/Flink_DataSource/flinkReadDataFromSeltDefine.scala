package com.bigdata.flink.Flink_source_sink.Flink_DataSource

import com.bigdata.flink.comment.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random

object flinkReadDataFromSeltDefine {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 自定义souce
    val stream4 = env.addSource(new SensorSource())

    // 输出，并设置并行度为1
    stream4.print("stream4:").setParallelism(1)

    //    启动流，不停止
    env.execute("SelfDefineDataSource")

  }
}

class SensorSource() extends SourceFunction[SensorReading]() {

  // 1、定义一个 flag ，表示数据是否正常运行,是否需要继续生产数据
  var running = true

  // 2、取消数据生成
  override def cancel(): Unit = {
    running = false
  }

  // 3、正常生成数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 3.1 初始化生成随机数据，间隔一定时间生成 流数据。
    val rand = new Random()

    var curTemp = 1.to(10).map(
      // 36度 加上高斯分布（正态分布）
      i => ("sensor_" + i, 36 + rand.nextGaussian() * 20)
    )

    // 3.2 使用无限循环生成 流数据
    while (running) {
      //  在前一次的温度值基础上 更新温度
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian())
      )
      // 获取当前时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        // 使用 sourceContext.collect 一条条的发出去
        t => sourceContext.collect(SensorReading(t._1, curTime / 1000, t._2))
      )
      // 间隔 0.5 秒
      Thread.sleep(500)

      //
    }
  }
}
