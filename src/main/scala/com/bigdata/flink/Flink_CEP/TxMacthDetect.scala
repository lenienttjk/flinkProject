package com.bigdata.flink.Flink_CEP

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, OutputTag}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._


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
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

object TxMacthDetect {

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
    val receiptEventStream = env
      .readTextFile("E:\\Project\\flinkProject\\dataSource\\ReceiptLog.csv")

      .map(line => {
        val dataArray = line.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      // 按事件时间
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)


    // 3.将两条流连接起来，共同处理
    val processedStream = orderEventStream.connect(receiptEventStream)
      .process(new TxPayMatch())

    //输出
    processedStream.print("mached successfully")

    //    侧输出流输出
    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")


    env.execute("tx match job")
  }


  // 2个输入流
  class TxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]() {

    // 1.0定义状态 保存订单事件
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))

    //1.1订单事件数据处理
    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 判断 有没有到账事件
      val receipt = receiptState.value()
      if (receipt != null) {
        collector.collect((pay, receipt))
      } else {
        // 如果 到账事件还没到
        payState.update(pay)

        ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5 * 1000L)
      }
    }


    // 2.0定义状态 保存 到账事件
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-State", classOf[ReceiptEvent]))

    //  2.1到账事件数据处理
    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

      // 判断 有没有到账事件
      val pay = payState.value()
      if (pay != null) {
        collector.collect((pay, receipt))
        payState.clear()
      } else {
        // 如果 到账事件还没到
        receiptState.update(receipt)

        ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 5 * 1000L)
      }
    }

    //  3、定时器触发
    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 到时间了，如果还没有收到某个事件的，输出报警
      if (payState.value() != null) {
        //      receipt 到账没来，输出pay 到侧输出流
        ctx.output(unmatchedPays, payState.value())
      }
      if (receiptState.value() != null) {
        ctx.output(unmatchedReceipts, receiptState.value())
      }

      //      清空操作
      payState.clear()
      receiptState.clear()
    }
  }

}










