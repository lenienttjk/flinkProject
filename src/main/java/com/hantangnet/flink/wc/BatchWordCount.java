package com.hantangnet.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import scala.Tuple2$;

/**
 * @Classname WordCount
 * @Description TODO 批处理
 * @Date 2022/2/7 15:17
 * @Created by tjk
 */

public class BatchWordCount {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        String inputPath = "/Users/tjk/Desktop/junbowork/junbo-flink/dataSource/words.txt";

        DataSource<String> stringDataSource = env.readTextFile(inputPath);

        AggregateOperator<Tuple2<String, Integer>> resultSet = stringDataSource.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);

        //  需要抛出异常
        resultSet.print();


    }


    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

            String words[] = value.split(" ");

            for (String word:words) {
               out.collect( new Tuple2<>(word,1));
            }
        }

    }
}
