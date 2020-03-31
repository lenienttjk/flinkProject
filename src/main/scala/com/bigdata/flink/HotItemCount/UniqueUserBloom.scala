//package com.bigdata.flink.HotItemCount
//
//import java.lang
//
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//import redis.clients.jedis.Jedis
//
//
///**
//  * @Auther: tjk
//  * @Date: 2020-02-22 12:12
//  * @Description:
//  *
//  * 使用布隆过滤器 进行用户的去重，得到独立用户访问
//  * 使用 redis 依赖
//  */
//
//
//object UniqueUserBloom {
//  def main(args: Array[String]): Unit = {
//
//    // 创建一个env
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    // 显式地定义Time类型
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)
//
//    val dataStream: DataStream[Nothing] = env
//      .readTextFile("E:\\Project\\flinkProject\\dataSource\\UserBehavior.csv")
//      .map(line => {
//        val linearray = line.split(",")
//        UserBehavior(linearray(0).trim.toLong, linearray(1).trim.toLong, linearray(2).trim.toInt, linearray(3).trim, linearray(4).trim.toLong)
//      })
//      // 分配时间戳
//      .assignAscendingTimestamps(_.timestamp * 1000L)
//      // 只统计pv 操作
//      .filter(_.behavior == "pv")
//
//      // 输入 为二元组（String，Long）
//      .map(data => ("dumyKey", data.userId))
//      .keyBy(_._1)
//      .timeWindow(Time.hours(1))
//      .trigger(new MyTriger())
//
//      .process(new UvCountWithBloom())
//
//    dataStream.print("Uv Count")
//
//
//    // 调用execute执行任务
//    env.execute("UvCount WithBloom  Job")
//
//  }
//}
//
//// 自定义 窗口触发器
//class MyTriger() extends Trigger[(String, Long), TimeWindow] {
//
//  // 重写方法1  onEventTime
//  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
//    TriggerResult.CONTINUE
//  }
//
//  // 重写方法2 onProcessingTime
//  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
//    TriggerResult.CONTINUE
//  }
//
//  // 重写方法3  onElement
//  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
//    // 每来一条数据处理一条数据，并清空窗口操作
//    TriggerResult.FIRE_AND_PURGE
//  }
//
//  // 重写方法4  clear,没有定义定时器，不用管
//  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
//}
//
//
//// 自定义 布隆过滤器
//class Bloom(size: Long) extends Serializable {
//
//  // 位图大小 16M
//  private val cop = if (size > 0) size else 1 << 27
//
//  // 定义哈希函数   value 输入的key
//
//  def hash(value: String, sead: Int): Long = {
//    var result: Long = 0L
//
//    for (i <- 0 until value.length) {
//      result = result * sead + value.charAt(i)
//    }
//
//    result & (cop - 1)
//  }
//}
//
//
//// 自定义处理 process()  IN, OUT, KEY, W
//class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
//
//  // 定义 redis 连接
//  lazy val jedis = new Jedis("mini1", 7001)
//  // 定义64 M 大小布隆过滤器，过滤 上亿的key
//  lazy val bloom = new Bloom(1 << 29)
//
//  override def process(key: String, context: ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]#Context, iterable: lang.Iterable[(String, Long)], collector: Collector[UvCount]): Unit = {
//    // 位图的存储方式
//    val storeKey = context.window.getEnd.toString
//    var count = 0L
//
//    // 把每个窗口的uv count 值也存入 redis， 先从 redis读取
//    if (jedis.hget("count", storeKey) != null) {
//
//      count = jedis.hget("count", storeKey).toLong
//
//    } else {
//      // 用布隆 过滤器 判断 当前用户是否已经存在
//      val userId = iterable.iterator().next()._2.toString
//      //      val userId = iterable.last._2.toString
//
//
//      val offset = bloom.hash(userId, 61)
//
//      // 定义一个标识
//      val isExist = jedis.getbit(storeKey, offset)
//      if (!isExist) {
//        // 如果不存在，位图对应位置置一
//        jedis.setbit(storeKey, offset, true)
//
//        jedis.hset("count", storeKey, (count + 1).toString)
//
//        // 在流里输出
//        collector.collect(UvCount(storeKey.toLong, count + 1))
//      } else {
//        // 如果存在
//        collector.collect(UvCount(storeKey.toLong, count))
//      }
//
//    }
//  }
//}
