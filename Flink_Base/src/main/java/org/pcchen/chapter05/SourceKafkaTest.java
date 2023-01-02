package org.pcchen.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 从kafka中获取数据
 *
 * @author: ceek
 * @create: 2023/1/2 0:12
 */
public class SourceKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.101:9092");
        properties.setProperty("group.id", "consumer-group6");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("auto.offset.reset", "earliest");

        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>(
                "first",
                new SimpleStringSchema(),
                properties
        ));

        stream.print("Kafka");

        env.execute();
    }
}
