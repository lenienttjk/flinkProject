package com.hantangnet.flink.cdc;

/**
 * @Classname MySqlBinlogSourceExample
 * @Description TODO
 * @Date 2021/10/27 21:42
 * @Created by tjk
 */

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


public class MySqlBinlogSourceExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SourceFunction<JSONObject> sourceFunction = MySqlSource.<JSONObject>builder()
                .hostname("localhost")
                .port(3306)
                // monitor all tables under inventory database
                .databaseList("demo")

                .username("root")
                .password("123tjk123TJK@#￥")
                // converts SourceRecord to String
                .deserializer(new CdcDwdDeserializationSchema())
                .build();


        DataStreamSource<JSONObject> stringDataStreamSource = env.addSource(sourceFunction);

        stringDataStreamSource.print("===>");


        try {
            env.execute("测试mysql-cdc");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
