package com.bigdata.flink.ScalaTool

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Auther: tjk
  * @Date: 2020-06-26 14:27
  * @Description:
  */

object Date2timeStamp {


    def main(args: Array[String]): Unit = {
      val dtString = "2020-01-10 10:20:30";
      val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      val timestamp = dateToTimestamp(dtString, sdf);
      println(timestamp); //1578622830000
      val dt = timestampToDate(timestamp, sdf);
      println(dt); //2020-01-10 10:20:30


      val dtString2 = "2019-12-12";
      val sdf2 = new java.text.SimpleDateFormat("yyyy-MM-dd");

      val timestamp2 = dateToTimestamp(dtString2, sdf2);
      println(timestamp2); //1576080000000
      val dt2 = timestampToDate(timestamp2, sdf2);
      println(dt2); //2019-12-12
    }

    /**
      * 日期转时间戳
      **/
    def dateToTimestamp(dtString: String, sdf: java.text.SimpleDateFormat): Long = {
      val dt = sdf.parse(dtString);
      val timestamp = dt.getTime();
      return timestamp;
    }


    /**
      * 时间戳转日期
      **/
    def timestampToDate(timestamp: Long, sdf: java.text.SimpleDateFormat): String = {
      val dtString = sdf.format(timestamp);
      return dtString;
    }

  }
