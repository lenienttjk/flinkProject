package com.bigdata.flink.FlinkCep


import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/*
使用状态 编程 CEP 方式
需要保存状态 ：

例如： 来一个create, 接下来的应该是pay,定一个定时器，否则超时
但是实际上 会有 先来pay,再来create的订单 ，也就是实时的输入由于各种原因
数据会乱序
 */

object OrderOutTime_StateWithCEP {
  def main(args: Array[String]): Unit = {
    //  1、创建一个env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 显式地定义Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //    // 读取文件方式1,把文件放到 resource 下
    //    val resource = getClass.getResource("/OrderLog.csv")
    //    val dataStream = env.readTextFile(resource.getPath)

    ///2、读取文件方式2
    // 1、读取订单事件数据
    val orderEventStream = env
      .readTextFile("E:\\Project\\flinkProject\\dataSource\\OrderLog.csv")

      .map(line => {
        val dataArray = line.split(",")
        OrderEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      // 按事件时间
      .assignAscendingTimestamps(_.eventTime * 1000L)

      // 针对同一订单ID,进行key by， 来一条create,接下来应该 pay
      .keyBy(_.OrderId)



    ///3、定义process function  进行超时检测
    val timeoutWarningStream =
      orderEventStream.process(
        new OrderTimeOutWarning()
      )

    // 4、输出
    timeoutWarningStream.print()


    env.execute("orderTimeOut  job with CEP ")
  }
}

// 实现自定义处理函数
class OrderTimeOutWarning() extends KeyedProcessFunction[String, OrderEvent, OrderResult]() {
  //   定义一个状态 ：支付事件是否来过
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayedState", classOf[Boolean]))

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[String, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {

    //    1、取出状态标识
    val isPayed = isPayedState.value()
    // 此为默认一个create,一个payed
    if (value.eventType == "create" && !isPayed) {
      //如果订单状态是 create,并且没有支付过,单位：毫秒
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 15 * 60 * 1000L)

    } else if (value.eventType == "pay") {
      // 直接把状态改为true
      isPayedState.update(true)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 判断ispayed 是否为true
    val isPayed = isPayedState.value()
    if (isPayed) {
      out.collect(OrderResult(ctx.getCurrentKey.toLong, "order payed successfully"))
    } else {
      out.collect(OrderResult(ctx.getCurrentKey.toLong, "order time out"))
    }
    // 清空状态
    isPayedState.clear()
  }
}
