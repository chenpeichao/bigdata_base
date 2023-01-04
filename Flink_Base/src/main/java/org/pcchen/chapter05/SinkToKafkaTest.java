package org.pcchen.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * 数据写入到kafka示例
 *
 * @author: ceek
 * @create: 2023/1/4 17:14
 */
public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(4);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.101:9092");
//        properties.setProperty("group.id", "consumer-group6");
//        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
////        properties.setProperty("auto.offset.reset", "latest");
//        properties.setProperty("auto.offset.reset", "earliest");

        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>("192.168.1.101:9092", "events", new SimpleStringSchema());

        kafkaStream.print();

        //将event转换成string写入文件
        kafkaStream.map(line -> {
            String[] events = line.split(",");
            return new Event(events[0].trim(), events[1].trim(), Long.valueOf(events[2].trim())).toString();
        })
                .addSink(kafkaSink);

        env.execute();
    }
}
