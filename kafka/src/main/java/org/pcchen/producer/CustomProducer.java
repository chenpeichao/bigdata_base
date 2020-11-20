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

//    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "10.10.32.61:9092");//kafka集群，broker-list
//        props.put("acks", "all");
//        props.put("retries", 1);//重试次数
//        props.put("batch.size", 16384);//批次大小
//        props.put("linger.ms", 1);//等待时间
//        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//
//        Producer<String, String> producer = new KafkaProducer<String, String>(props);
//        for (int i = 0; i < 100; i++) {
//            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i)));
//        }
//        producer.close();
//    }

    public static void main(String[] args) {
        Properties conf = new Properties();
//        conf.put("--broker-list","192.168.0.30:9094");
        conf.put("bootstrap.servers", "10.10.32.60:9092");
//        conf.put("zookeeper","192.168.0.28:2181");
        conf.put("acks", "all");
//        conf.put("topic", args[0]);
        conf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        conf.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        KafkaProducer kafkaClient = new KafkaProducer<String, String>(conf);
        for (int i = 0; i < 10; i++) {
            kafkaClient.send(new ProducerRecord<String, String>("number", Integer.toString(i), Integer.toString(i + 1)));
            System.out.println("HIVE SEND ONE MESSAGE");
        }
        kafkaClient.close();
    }

}
