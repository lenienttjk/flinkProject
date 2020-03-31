package com.bigdata.flink.Flink_CEP

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._


object OrderOutTime_CEP {
  def main(args: Array[String]): Unit = {
    // 创建一个env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 显式地定义Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //    // 读取文件方式1,把文件放到 resource 下
    //    val resource = getClass.getResource("/OrderLog.csv")
    //    val dataStream = env.readTextFile(resource.getPath)

    // 读取文件方式2
    // 1、读取订单事件数据
    val orderEventStream = env
      .readTextFile("E:\\Project\\flinkProject\\dataSource\\OrderLog.csv")

      .map(line => {
        val dataArray = line.split(",")
        OrderEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      // 按事件时间
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.OrderId)


    // 2、定义匹配模式
    val orderPayPatten = Pattern
      // 第一次创建 create
      .begin[OrderEvent]("begin")
      .where(_.eventType == "create")

      // 第二次 pay ,不是严格的模式，不是紧跟create
      .followedBy("follow")
      .where(_.eventType == "pay")

      // 事件限制：15分钟之内
      .within(Time.minutes(15))

    //3、在事件流上应用 模式, 传入上面2个参数
    val patternStream =
      CEP.pattern(orderEventStream, orderPayPatten)


    // 4、得到查询匹配事件,超时事件做报警提示
    val orderTimeOutputTag = new OutputTag[OrderResult]("orderTimeout")


    val resultStream = patternStream.select(
      orderTimeOutputTag,
      new OrderTimeOutSelect(),
      new OrderPaySelect()
    )


    // 5、打印输出
    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeOutputTag).print("TimeOut")


    env.execute("orderTimeOut  job with CEP ")
  }
}

// 自定义超时事件序列 Map 中保存
// 模式匹配 select
class OrderTimeOutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {

  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {

    // 从 map中取出 对应事件
    val timeoutOrderId = map.get("begin").iterator().next().OrderId.toLong
    // 输出
    OrderResult(timeoutOrderId, "timeout fail")
  }
}


class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult]() {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    // 从 map中取出 对应事件
    val payOrderId = map.get("follow").iterator().next().OrderId.toLong
    // 输出
    OrderResult(payOrderId, "payed successfully")
  }
}
