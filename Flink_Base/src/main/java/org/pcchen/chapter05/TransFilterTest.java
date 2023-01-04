package org.pcchen.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * filter算子示例类
 *
 * @author: ceek
 * @create: 2023/1/4 10:02
 */
public class TransFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 3000L)
        );

        stream.filter(event -> {
            if(event.getName().equals("Mary")) {
                return true;
            } else {
                return false;
            }
        }).print();

        stream.filter(event -> event.getName().equals("Mary")).print();

        env.execute();
    }
}
