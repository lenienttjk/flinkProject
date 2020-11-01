package com.bigdata.flink.comment

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Auther: tjk
  * @Date: 2020-02-19 14:32
  * @Description:
  */
object MainDemo {
  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置输入数据的并行度
    env.setParallelism(1)

    // 设置检查点 ，可以传入 检查模式
    //    EXACTLY_ONCE, 精确一次
    //    AT_LEAST_ONCE; 至少一次
    env.enableCheckpointing(60000)

    // 设置级别  默认 EXACTLY_ONCE
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // 设置超时时间
    env.getCheckpointConfig.setCheckpointTimeout(100000)

    // 设置 检查点保存失败，是否让 整个job失败
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)

    // 设置checkPoint 同时数量,默认1
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    // 设置 2次checkpoint 最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)

    // 开启外部 checkpoint, 手动取消job 是否需要保存外部 checkpoint
    env.getCheckpointConfig.
      enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)


    // 重启策略,设置重启次数 60次
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10, TimeUnit.SECONDS)))
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(300), Time.seconds(10)))
    // 开启状态后端
    //    val checkpointPath: String = ""
    //    val backkend = new RocksDBStateBackend(checkpointPath)
    ////    env.setStateBackend(backkend)


    // 启动
    env.execute("MainDemo")


  }
}
