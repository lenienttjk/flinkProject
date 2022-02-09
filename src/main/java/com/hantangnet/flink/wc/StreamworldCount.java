package com.hantangnet.flink.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Classname StreamworldCount
 * @Description TODO
 * @Date 2022/2/7 15:52
 * @Created by tjk
 */

public class StreamworldCount {
    public static void main(String[] args) throws Exception {
        // 创建环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        String inputPath = "/Users/tjk/Desktop/junbowork/junbo-flink/dataSource/words.txt";

        DataStreamSource<String> stringDataSource = env.readTextFile(inputPath);


        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = stringDataSource.flatMap(new BatchWordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);


        resultStream.print();


        env.execute();


    }
}
