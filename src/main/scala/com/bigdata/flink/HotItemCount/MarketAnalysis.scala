package com.bigdata.flink.HotItemCount

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.commons.net.ntp.TimeStamp
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * @Auther: tjk
  * @Date: 2020-02-24 10:35
  * @Description:
  */

object MarketAnalysis {
  def main(args: Array[String]): Unit = {
    // 1、创建一个env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    // 显式地定义Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    ///2、输入数据源 new SimulatedEventSource()
    val dataStream = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UnInstall")
      .map(data => {
        // 渠道和行为类型作为key, 都是 (String,String)
        ((data.channel, data.behavior), 1L)
      })

      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .process(new MarktingCountByChannel())

    dataStream.print("dataStream")


    env.execute("channel job")
  }
}


// 1、自定义输入数据样例类
case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

// 2 、自定义数据源
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior] {
  // 定义是否运行的标识
  var running = true

  // 定义用户行为集合,点击，下载，安装，卸载
  val behaviorTypes: Seq[String] = Seq("Click", "Download", "Install", "UnInstall")
  // 定义渠道聚合
  val channelSets: Seq[String] = Seq("wechart", "weibo", "appstore", "xiaomiStore")
  // 定义随机发生器
  val rand: Random = new Random()

  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    // 定义生成数据的限制
//    val maxElements = Long.MaxValue
    val maxElements = 1000L
    var count = 0L
    // 随机生成
    while (running && count < maxElements) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSets(rand.nextInt(channelSets.size))
      val ts = System.currentTimeMillis()
      // 输出
      sourceContext.collect(MarketingUserBehavior(id, behavior, channel, ts))
      count += 1
      // 生成数据 缓一缓
      TimeUnit.MILLISECONDS.sleep(10L)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}


// 3、 自定义输出数据样例类
case class MarketingViewCount(windowStart: String, windowEnd: String,
                              channel: String, behavior: String, count: Long)

//  4 、自定义处理类   IN, OUT, KEY, W
class MarktingCountByChannel() extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {

  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {

    val startTs = new TimeStamp(context.window.getStart).toString
    val endTs = new TimeStamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size

    out.collect(MarketingViewCount(startTs, endTs, channel, behavior, count))
  }
}













