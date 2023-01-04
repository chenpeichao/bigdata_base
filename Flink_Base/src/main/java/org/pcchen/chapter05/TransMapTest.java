package org.pcchen.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * map算子示例
 *
 * @author: ceek
 * @create: 2023/1/4 9:35
 */
public class TransMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 3000L)
        );

        //1、通过自定义类实现MapFunction
        stream.map(new MyMapFunction()).print("MyMapFunction:");

        //2、通过匿名类实现MapFunction
        stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.getName();
            }
        }).print("niminghanshu");

        //3、使用lambda表达式
        stream.map(event -> event.getName()).print();

        env.execute();
    }

    public static class MyMapFunction implements MapFunction<Event, String> {
        @Override
        public String map(Event event) throws Exception {
            return event.getName();
        }
    }
}
