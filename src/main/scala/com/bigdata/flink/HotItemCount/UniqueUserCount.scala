package com.bigdata.flink.HotItemCount

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Auther: tjk
  * @Date: 2020-02-22 12:12
  * @Description:
  */

object UniqueUserCount {
  def main(args: Array[String]): Unit = {

    // 创建一个env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 显式地定义Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream = env
      .readTextFile("E:\\Project\\flinkProject\\dataSource\\UserBehavior.csv")
      .map(line => {
        val linearray = line.split(",")
        UserBehavior(linearray(0).trim.toLong, linearray(1).trim.toLong, linearray(2).trim.toInt, linearray(3).trim, linearray(4).trim.toLong)
      })
      // 分配时间戳
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountWindow())


    dataStream.print(" Unique User Count")


    // 调用execute执行任务
    env.execute("UniqueUserCount Job")

  }
}

// 输入，输出，时间窗口
case class UvCountWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {

    // 定义 set ,去重UserID，set存入的是内存，可能程序挂掉，
    var idSet = Set[Long]()
    // 把窗口所有数据收集到set中，最后输出Set大小
    for (userbehavior <- input) {
      idSet += userbehavior.userId
    }
    // 可以放到 redis 去重，可以用布隆过滤器，解决数据去重过大
    out.collect(UvCount(window.getEnd, idSet.size))

  }
}
