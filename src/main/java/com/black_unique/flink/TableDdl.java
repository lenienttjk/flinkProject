package com.black_unique.flink;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * @Auther: tjk
 * @Date: 2020-07-25 14:36
 * @Description:
 */
public class TableDdl {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("CREATE TABLE transactions (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'transactions',\n" +
                "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
                "    'format'    = 'csv'\n" +
                ")"
        );


        tEnv.executeSql("CREATE TABLE spend_report (\n" +
                "    account_id BIGINT,\n" +
                "    log_ts     TIMESTAMP(3),\n" +
                "    amount     BIGINT\n," +
                "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
                ") WITH (\n" +
                "   'connector'  = 'jdbc',\n" +
                "   'url'        = 'jdbc:mysql://mysql:3306/sql-demo',\n" +
                "   'table-name' = 'spend_report',\n" +
                "   'driver'     = 'com.mysql.jdbc.Driver',\n" +
                "   'username'   = 'sql-demo',\n" +
                "   'password'   = 'demo-sql'\n" +
                ")");

        Table transactions = tEnv.from("transactions");
        TableResult report = (transactions).executeInsert("spend_report");
        report.print();


    }
}
