package com.bigdata.flink.FlinkCep


// 输入样例类
case class OrderEvent(OrderId: String, eventType: String, txId: String, eventTime: Long)

//输出样例类
case class OrderResult(OrderId: Long, resultMsg: String)
