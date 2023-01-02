package org.pcchen.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 生产者
 *
 * @author ceek
 * @create 2020-01-06 16:25
 **/
public class CustomProducer {
    public static void main(String[] args) {
        Properties conf = new Properties();
        conf.put("bootstrap.servers", "192.168.1.101:9092");
        conf.put("acks", "all");
        conf.put("retries", 1);//重试次数
        conf.put("batch.size", 16384);//批次大小
        conf.put("linger.ms", 1);//等待时间
        conf.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        conf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        conf.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String topic = "topic_spark_direct_10";
        KafkaProducer kafkaClient = new KafkaProducer<String, String>(conf);
        for (int i = 0; i < 3; i++) {
            kafkaClient.send(new ProducerRecord<String, String>(topic, Integer.toString(i), "测试数据" + Integer.toString(i + 1)));
            System.out.println("HIVE SEND ONE MESSAGE");
        }
        kafkaClient.close();
    }

}
