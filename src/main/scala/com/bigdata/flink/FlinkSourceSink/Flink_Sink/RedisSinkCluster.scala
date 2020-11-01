package com.bigdata.flink.FlinkSourceSink.Flink_Sink

import java.net.InetSocketAddress
import java.util
import java.util._

import com.bigdata.flink.comment.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


/**
  * 将 flink 处理过的 数据 保存到 Redis
  */
object RedisSinkCluster {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 如果不设，默认为本机核数的并行度，即读取数据安顺序一条条读取
    env.setParallelism(1)

    // 1、读取数据,从kafka读取

    //  kafka 配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "mini1:9092,mini2:9092,mini3:9092")
    properties.setProperty("group.id", "g1")
    properties.setProperty("key.serialization", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.serialization", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 2.从kafka 读取数据
    //  指定 topic ,kafka 配置
    // 输入 topic sensorInput
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensorInput", new SimpleStringSchema(), properties))


    // 3、转换操作
    val dataStream = inputStream.map(data => {
      val daraArray = data.split(",")
      val sensorId = daraArray(0).trim
      val timeStamp = daraArray(1).trim.toLong
      val tempperture = daraArray(2).trim.toDouble

      //  包装成样例类
      SensorReading(sensorId, timeStamp, tempperture)
    })


    // 4、从文件读取数据，sink 到 Redis
    // Redis 配置
    //  连接redis集群 导入
    /*
    import java.net.InetSocketAddress
    import java.util
    import java.util._
     */
    var redisClusterNodes: Set[InetSocketAddress] = new util.HashSet[InetSocketAddress]()
    redisClusterNodes.add(new InetSocketAddress("mini1", 7001))
    redisClusterNodes.add(new InetSocketAddress("mini1", 7002))
    redisClusterNodes.add(new InetSocketAddress("mini1", 7003))
    redisClusterNodes.add(new InetSocketAddress("mini1", 7004))
    redisClusterNodes.add(new InetSocketAddress("mini1", 7005))
    redisClusterNodes.add(new InetSocketAddress("mini1", 7006))

    val conf = new FlinkJedisClusterConfig.Builder().setNodes(redisClusterNodes).build()


    // sink 到redis
    dataStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapperCluster))



    //    5 、启动流，不停止
    env.execute("RedisSinkTest")

  }
}

//  继承 RedisMapper
class MyRedisMapperCluster extends RedisMapper[SensorReading] {

  override def getCommandDescription: RedisCommandDescription = {
    // redis 的key 为 sensor_temperature
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  override def getKeyFromData(t: SensorReading): String = {
    t.tempPerture.toString
  }

  // 获取 值
  override def getValueFromData(t: SensorReading): String = {
    t.tempPerture.toString
  }
}


/*
启动kafka 生产者
./bin/kafka-console-producer.sh --broker-list mini1:9092,mini2:9092,mini3:9092 --topic sensorInput


启动 Redis，集群
脚本: RedisStart.sh


  启动集群的某台redis客户端
/usr/local/redis/src/redis-cli -c -h mini1 -p 7001


hgetall sensor_temperature

FlinkJedisPoolConfig	    单Redis服务器	适用于本地、测试场景
FlinkJedisClusterConfig	  Redis集群
FlinkJedisSentinelConfig	Redis哨兵
 */