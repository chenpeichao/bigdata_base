package org.pcchen.customer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 消费者
 *
 * @author ceek
 * @create 2020-01-10 11:06
 **/
public class CustomConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // kafka servers
        props.put("bootstrap.servers", "192.168.1.101:9092");
        // group
        props.put("group.id", "test-consumer-group");//消费者组，只要group.id相同，就属于同一个消费者组
        props.put("auto.commit.interval.ms", "1000");
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put("enable.auto.commit", "false");//自动提交offset
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        // 订阅的topic
        consumer.subscribe(Arrays.asList("number"));
        while (true) {
            // 超时时间 ms
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("测试 offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
                        record.value());
        }
    }
}
