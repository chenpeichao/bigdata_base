package org.pcchen.producer;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.ZKUtil;

import java.util.Properties;

/**
 * 测试生产者
 *
 * @author ceek
 * @create 2020-11-12 13:45
 **/
public class TestProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "10.10.32.60:2181");//kafka集群broker-list
        properties.setProperty("acks", "all");//
        properties.setProperty("retries", "1");//重试次数
        properties.put("batch.size", 16384);//批次大小，默认大小16k
        properties.put("linger.ms", 1); //等待时间
        properties.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小

        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer(properties);

//        AdminUtils.createTopic
//
//        AdminClient.create

        for(int i = 0; i < 100; i++) {
            kafkaProducer.send(new ProducerRecord("test_api_topic", "key_" + i, "value_"+i));
        }
    }

    public Properties createTopicProperties(String topicName, String partition, String replication) {
        Properties properties = new Properties();
        properties.put("topicName", "test_api_topic");
        properties.put("partition", "3");
        properties.put("replication", "1");
        return properties;
    }
}