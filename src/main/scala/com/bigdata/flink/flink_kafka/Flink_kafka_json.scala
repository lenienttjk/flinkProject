package com.bigdata.flink.flink_kafka

import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.api.scala._



object Flink_kafka_json {
  def main(args: Array[String]): Unit = {
    // 启动入口
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 非常关键，一定要设置启动检查点！！
    env.enableCheckpointing(5000)


    //配置kafka信息
    val props = new Properties()
    props.setProperty("bootstrap.servers", "mini1:9092,mini2:9092,mini3:9092")
    props.setProperty("zookeeper.connect", "mini1:2181,mini2:2181,mini3:2181")
    props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("group.id", "test")
    props.setProperty("auto.offset.reset", "latest")

    //读取数据 kafka 0.8版本
    //    val consumer = new FlinkKafkaConsumer08[String]("log", new org.apache.flink.api.common.serialization.SimpleStringSchema(), props)

    //读取数据 kafka 1.0版本
    val consumer = new FlinkKafkaConsumer010[String]("flinkKafka", new org.apache.flink.api.common.serialization.SimpleStringSchema(), props)

    //设置只读取最新数据
    consumer.setStartFromLatest()



    //添加kafka为数据源
    case class WordWithCount(world: String, count: Long)

    // 转换为json
        val stream = env.addSource(consumer).map(
          x => JSON.parseObject(x)
        )
          .map { w => WordWithCount(w.getString("ispname"), 1) }
          .keyBy("world")
          //      处理时间，翻滚窗口,去掉timeWindow 可以实现来一条数据就累加一条，
//          实现了按key 累加聚合的功能，即spark updateStateByKey的功能 mapWithState
//          .timeWindow(Time.seconds(5), Time.seconds(1))
          .sum("count")

//    val stream = env.addSource(consumer)



    // 打印数据 或保存数据
    stream.print().setParallelism(1)

    // 启动
    env.execute("Kafka_Flink")
  }
}

/*
结果：来一条，叠加一条
1> WordWithCount(电信,1)
3> WordWithCount(移动,1)
3> WordWithCount(移动,2)
3> WordWithCount(移动,3)
1> WordWithCount(联通,1)
3> WordWithCount(移动,4)
1> WordWithCount(电信,2)
3> WordWithCount(移动,5)
1> WordWithCount(联通,2)
1> WordWithCount(电信,3)
1> WordWithCount(联通,3)
3> WordWithCount(移动,6)
3> WordWithCount(移动,7)
1> WordWithCount(联通,4)
3> WordWithCount(移动,8)
3> WordWithCount(移动,9)
3> WordWithCount(OPERATOROTHER,1)
3> WordWithCount(移动,10)
 */