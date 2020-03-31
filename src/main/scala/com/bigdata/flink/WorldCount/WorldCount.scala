package com.bigdata.flink.WorldCount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

object WorldCount {
  def main(args: Array[String]): Unit = {
    //前提：
    // ①拦截非法的参数
    if (args == null || args.length != 4) {
      println(
        """
          |警告！请录入参数！ --input <源的path> --output <目的地的path>
        """.stripMargin)
      sys.exit(-1)
    }


    //②获得待计算的源的path，以及计算后的目的地的path
    val tool = ParameterTool.fromArgs(args)
    val inputPath = tool.get("input")
    val outputPath = tool.get("output")


    //步骤：

    //①执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //②计算，显示，或是保存结果
    //a）导入单例类scala中的隐式成员
    import org.apache.flink.api.scala._

    //b)迭代计算
    env.readTextFile(inputPath)
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .writeAsText(outputPath,FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1) //设置最后一个算子writeAsText的并行度

    //c)正式去执行，如果读取文件读取完成，则自动完成一个job
    env.execute(this.getClass.getSimpleName)

  }
}
/*
打包成jar包，提交到flink 集群运行命令
/root/input.txt 为linux的本地路径

./bin/flink run \
-c com.bigdata.flinkWC.WorldCount \
/root/wc.jar \
--input /root/input.txt \
--output /root/output.txt

 */