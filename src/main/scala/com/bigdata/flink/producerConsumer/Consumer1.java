package com.bigdata.flink.producerConsumer;//package com.producerConsumer;
//
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.Producer;
//
//import java.util.Properties;
//
///**
// * kafak  1.0 api
// * KafkaConsumer
// */
//public class Consumer1 {
//
//    //指定消费的主题（哪个类别的消息）
//    private static final String topic = "test";
//    //指定线程个数
//    private static final Integer thread = 2;
//
//    public static void main(String[] args) {
//        Properties props = new Properties();
//
//        props.put("bootstrap.servers", "mini1:9092,mini2:9092,mini3:9092");
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        Producer<String, String> producer = new KafkaProducer<String, String>(props);
//
//
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
//
//
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(1000);
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
//            }
//        }
//    }
//}
//
