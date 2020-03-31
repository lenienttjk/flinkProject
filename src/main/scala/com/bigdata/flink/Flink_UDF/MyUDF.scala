package com.bigdata.flink.Flink_UDF

import com.bigdata.flink.comment.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
转换算子
map
flatmap
filter
KeyBy  将数据分区，看似一个流拆成两个，实际还是一个流

Rolling Aggregation  : 针对 KeyedStream 后操作
如：sum() min() max()  minBy() maxBy()

Reduce 算子
split(分流：多流转换算子) 和select

Connect 合流 CoMap 

 */


object MyUDF {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 如果不设，默认为本机核数的并行度，即读取数据安顺序一条条读取
    env.setParallelism(1)

    // 1、读取数据
    val streamFromFile = env.readTextFile("E:\\Project\\flinkProject\\data.txt")

    // 2、转换操作
    val dataStream: DataStream[SensorReading] = streamFromFile.map(data => {
      val daraArray = data.split(",")
      val sensorId = daraArray(0).trim
      val timeStamp = daraArray(1).trim.toLong
      val tempperture = daraArray(2).trim.toDouble

      SensorReading(sensorId, timeStamp, tempperture)
    })


      // 3 更改需求：输出 传感器的温度+10， 时间戳是上一次数据的时间+1
      .keyBy("id")
      .reduce((x, y) => SensorReading(x.id, x.timeStamp + 1, y.tempPerture + 10))


    /*    // 4 多流转换 ，过时
        val splitStream = dataStream.split(data => {
          if (data.tempPerture > 30) Seq("hight") else Seq("low")
        })
        // select 可以传入多个参数
        val hight = splitStream.select("hight")
        val low = splitStream.select("low")
        val all = splitStream.select("hight", "low")

        // 5.输出
        hight.print("hight")
        low.print("low")
        all.print("all")*/


    //  函数类
    dataStream.filter(new MyFilter()).print("Stream")
    // 等同 匿名函数
    dataStream.filter(_.id.startsWith("sensor")).print("Stream")
    // 等同 有 =>
    dataStream.filter(data => data.id.startsWith("sensor")).print("Stream")


    env.execute("MyUDF")

  }
}

class MyFilter() extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = {
    //      t.id.contains("flink")
    t.id.startsWith("sensor_1")
  }
}


// RichMapFunction() 2个参数 输入 ，输出
class MyMap() extends RichMapFunction[SensorReading, String] {

  override def map(in: SensorReading): String = {
    "flink"
  }
}