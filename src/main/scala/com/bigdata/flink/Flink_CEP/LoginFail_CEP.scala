package com.bigdata.flink.Flink_CEP


import java.util

import com.bigdata.flink.HotItemCount.{LoginEvent, Warning}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
  * @Auther: tjk
  * @Date: 2020-02-24 15:19
  * @Description:
  */

object LoginFail_CEP {
  def main(args: Array[String]): Unit = {
    // 创建一个env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 显式地定义Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)
    // 输入并行度
    env.setParallelism(1)
    // 分布式快照间隔 时间
    env.enableCheckpointing(2000)

    // 1、读取事件数据
    val loginEventStream = env
      .readTextFile("E:\\Project\\flinkProject\\dataSource\\LoginLog.csv")

      .map(line => {
        val dataArray = line.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })

      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(t: LoginEvent): Long = t.eventTime * 1000L
      })

      .keyBy(_.userId)


    // 2、定义匹配模式
    val loginFailPatten = Pattern.begin[LoginEvent]("begin")
      // 第一次失败
      .where(_.eventType == "fail")
      // 第二次失败
      .next("next")
      .where(_.eventType == "fail")
      // 事件限制：2秒之内
      .within(Time.seconds(2))

    //3、在事件流上应用 模式, 传入上面2个参数
    val patternStream = CEP.pattern(loginEventStream, loginFailPatten)


    // 4、得到查询匹配事件
    val loginFailDataStream = patternStream.select(new LoginFailMatch())


    // 5、打印输出
    loginFailDataStream.print()


    env.execute(" login fail job with CEP ")
  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    // 从 map中取出 对应事件
    val firstFail = map.get("begin").iterator().next()
    val lastFail = map.get("next").iterator().next()
    Warning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail")
  }
}
