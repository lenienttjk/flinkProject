package com.bigdata.flink.producerConsumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

/**
 * kafak  1.0 api
 * KafkaProducer
 */
public class Producer1 {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "mini1:9092,mini2:9092,mini3:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);


        for (int i = 0; i <= 100; i++) {

            Scanner sc = new Scanner(System.in);
            while (sc.hasNext()) {

                String message = sc.next();

                producer.send(new ProducerRecord<String, String>("test", message));
            }

//            producer.send(new KeyedMessage<String,String>("test","message"));
        }
    }
}
