package org.pcchen.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * spark的kafka生产者
 *
 * @author ceek
 * @create 2021-01-19 11:08
 **/
public class SparkKafkaProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.10.32.60:9093,10.10.32.61:9093,10.10.32.62:9093");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String topic = "topic_spark_direct_10";
        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        //数据发送
        boolean flag = true;
        while (flag) {
            for (int i = 0; i < 10; i++) {
                kafkaProducer.send(new ProducerRecord(topic, "字符串" + i));
            }

            kafkaProducer.flush();
            Thread.sleep(2000);
        }

        kafkaProducer.close();
    }
}
