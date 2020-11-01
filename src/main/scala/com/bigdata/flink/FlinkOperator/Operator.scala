package com.bigdata.flink.FlinkOperator

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}

/**
  * @Auther: tjk
  * @Date: 2020-03-06 10:50
  * @Description:
  */
/*
operator 算子，flink 常用算子：都叫转换算子
1、DataStream转换算子
Map()         DataStream --> DataStream
flatMap()     DataStream --> DataStream
Filter()      DataStream --> DataStream
KedBy()       DataStream --> KeyedStream
reduce()      T --> T

union()       DataStream --> DataStream    多流---->单流（数据集类型格式必须一致）

Connect()                                  多流---->单流（数据集类型格 允许不同）

CoMap()       DataStream -->ConnectedStream --> DataStream

CoFlatMap()   DataStream -->ConnectedStream --> DataStream


split()      DataStream -->SplitStream --> DataStream

select()




 */
/*
map() 算子原理：
源码 ：  def map[R: TypeInformation](mapper: MapFunction[T, R]): DataStream[R] = { }
传入一个函数，返回 DataStream类型
R: TypeInformation  R代表一个类型，可以是整型，等等


flatMap()
源码： def flatMap[R: TypeInformation](fun: T => TraversableOnce[R]): DataStream[R] = { }
传入一个函数，返回 DataStream类型
R: TypeInformation  R代表一个类型，可以是整型，等等

 */

object Operator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //    env.readTextFile("")
    //      .flatMap(_.split("\\s+"))
    //      .map((_,1))

    var stream1 = env.fromElements(("a", 1), ("b", 2))
    var stream2 = env.fromElements(("c", 3), ("d", 4))


    // 合并2条流
    val result1: DataStream[(String, Int)] = stream1.union(stream2)


    var stream3 = env.fromElements(("A", 5), ("B", 6), ("C", 7))
    var stream4 = env.fromElements("a", "b", "c")

    //  [ ("A", 5),"a"],
    // 1、connect 是中间品，没有print() 方法
    val result2: ConnectedStreams[(String, Int), String] = stream3.connect(stream4)

    ///2、使用CoMap() ,传入2个函数
    val result3 = result2.map(
      t1 => {
        (t1._1, t1._2)
      },
      t2 => {
        (t2, 0)
      }
    )
    // 3、现在可以打印了
    result3.print()



    env.execute()
  }
}


