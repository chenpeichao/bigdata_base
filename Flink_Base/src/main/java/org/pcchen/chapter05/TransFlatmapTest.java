package org.pcchen.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 转换算子flatmap示例
 *
 * @author: ceek
 * @create: 2023/1/2 16:22
 */
public class TransFlatmapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 3000L)
        );

        //1、使用自定义类实现FlatMapFunction
        stream.flatMap(new MyFlatMap()).print("myflatmap:");

        //2、使用匿名函数实现FlatMapFunction
        stream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event event, Collector<String> out) throws Exception {
                out.collect(event.getName());
            }
        }).print("niminghanshu:");

        //3、通过lambda表达式
        stream.flatMap((Event event, Collector<String> out) -> {
            if(event.getName().equals("Mary")) {
                out.collect(event.getName());
            } else if(event.getName().equals("Bob")) {
                out.collect(event.getName());
                out.collect(event.getUrls());
                out.collect(event.getTimestamp().toString());
            }
        })//.returns(new TypeHint<String>() {})
                .returns(Types.STRING)
                .print("flatmaplambda:");

        env.execute();
    }
    public static class MyFlatMap implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            if (value.getName().equals("Mary")) {
                out.collect(value.getName());
            } else if (value.getName().equals("Bob")) {
                out.collect(value.getName());
                out.collect(value.getUrls());
            }
        }
    }
}
