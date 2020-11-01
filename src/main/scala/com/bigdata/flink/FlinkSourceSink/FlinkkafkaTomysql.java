package com.bigdata.flink.FlinkSourceSink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.scala.DataStream;

import java.io.InputStream;

/**
 * @Auther: tjk
 * @Date: 2020-06-25 23:50
 * @Description:
 */
public class FlinkkafkaTomysql {
    public static void main(String[] args) throws Exception {

        // 加载配置资源文件
        InputStream is = FlinkkafkaTomysql.class.getClassLoader().getResourceAsStream("config.properstis");
       // 利用 flink的参数工具
        ParameterTool paramters = ParameterTool.fromPropertiesFile(is);

        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream();






    }
}
