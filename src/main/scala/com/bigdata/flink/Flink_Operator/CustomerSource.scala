package com.bigdata.flink.Flink_Operator

import org.apache.flink.streaming.api.functions.source.SourceFunction

class CustomerSource() extends SourceFunction[ StationLog]() {

  override def run(sourceContext: SourceFunction.SourceContext[StationLog]): Unit = {

  }

  override def cancel(): Unit = {

  }
}

case class StationLog(id:String,callType:String,dt:String)
