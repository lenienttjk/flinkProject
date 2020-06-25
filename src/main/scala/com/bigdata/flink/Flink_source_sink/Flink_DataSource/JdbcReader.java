package com.bigdata.flink.Flink_source_sink.Flink_DataSource;

/**
 * @Auther: tjk
 * @Date: 2020-06-17 22:58
 * @Description:
 */

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.alibaba.fastjson.JSONObject;


/**
 * @Description mysql source
 * @Author jiangxiaozhi
 * @Date 2018/10/15 17:05
 **/
public class JdbcReader extends RichSourceFunction<Tuple2<String, String>> {
    private static final Logger logger = LoggerFactory.getLogger(JdbcReader.class);

    private Connection conn = null;
    private PreparedStatement ps = null;

    //该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        //加载数据库驱动
        Class.forName("com.mysql.jdbc.Driver");


        //获取连接
        conn = DriverManager
                .getConnection(
                        "jdbc:mysql://localhost:3307/gciantispider?useUnicode=true&characterEncoding=utf8",
                        "root",
                        "123456");
        ps = conn.prepareStatement("");
    }


    //执行查询并获取结果
    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        try {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String name = resultSet.getString("nick");
                String id = resultSet.getString("user_id");
                logger.error("readJDBC name:{}", name);

                JSONObject jsonObject = new JSONObject();
                jsonObject.put("name", "wjw");
                jsonObject.put("age", 22);
                jsonObject.put("sex", "男");
                jsonObject.put("school", "商职");
                String jsonStr = JSONObject.toJSONString(jsonObject);
                System.out.println(jsonStr);


                Tuple2<String, String> tuple2 = new Tuple2<>();
                tuple2.setFields(id, name);
                ctx.collect(tuple2);
                //发送结果，结果是tuple2类型，2表示两个元素，可根据实际情况选择
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }

    }

    //关闭数据库连接
    @Override
    public void cancel() {
        try {
            super.close();
            if (null != conn) {
                conn.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }
}