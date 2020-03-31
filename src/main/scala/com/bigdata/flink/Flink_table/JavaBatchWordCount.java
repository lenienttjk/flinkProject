package com.bigdata.flink.Flink_table;

/**
 * @Auther: tjk
 * @Date: 2020-03-31 13:16
 * @Description:
 */
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class JavaBatchWordCount {   // line:10

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        String path = JavaBatchWordCount.class.getClassLoader().getResource("words.txt").getPath();

        tableEnv.connect(new FileSystem().path(path))
                .withFormat(new OldCsv().field("word", Types.STRING).lineDelimiter("\n"))
                .withSchema(new Schema().field("word", Types.STRING))
                .registerTableSource("fileSource");  // line:20

        Table result = tableEnv.scan("fileSource")
                .groupBy("word")
                .select("word, count(1) as count");

        tableEnv.toDataSet(result, Row.class).print();
    }
}