package com.bigdata.flink.FlinkCep

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * @Auther: tjk
  * @Date: 2020-02-28 22:43
  * @Description:
  */

/*
实时对账

两条流的 连接 key 的类型需要一致，否则出错
txId  与 OrderId 类型 一致
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)
case class OrderEvent(OrderId: String, eventType: String, txId: String, eventTime: Long)


 */

object TxMacthByJoin {

  //侧输出流,全局变量
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")


  def main(args: Array[String]): Unit = {


    // 创建一个env，不要导错包
    // 导入 import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 显式地定义Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


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


    //2、 读取支付事件流
    val payEventStream = env
      .readTextFile("E:\\Project\\flinkProject\\dataSource\\ReceiptLog.csv")

      .map(line => {
        val dataArray = line.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      // 按事件时间
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)


    // 3.将两条流连接起来，共同处理,使用join
    val processedStream =
      orderEventStream
        .intervalJoin(payEventStream)
        .between(Time.seconds(-5), Time.seconds(5))
        .process(new TxPayMatchByJoin())



    //输出
    processedStream.print("mached successfully")


    env.execute("tx match job")
  }


}

class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]() {

  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    collector.collect((left, right))
  }
}

