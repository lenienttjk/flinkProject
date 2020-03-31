package com.bigdata.flink.Flink_Operator

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{SplitStream, StreamExecutionEnvironment}

/**
  * @Auther: tjk
  * @Date: 2020-03-06 15:57
  * @Description:
  */

object SplitAndSelect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

   val dataStream =  env.addSource(new CustomerSource())

    // 1、类型为 SplitStream，流没有切开
    val splitStream: SplitStream[StationLog] = dataStream.split(
      t => {
        if (t.callType.equals("success"))
          Seq("success")
        else
          Seq(" fail")
      }
    )

    // 2、select
    val stream1 = splitStream.select("success")
    val stream2 = splitStream.select("fail")



    env.execute()
  }
}
