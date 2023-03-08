package org.pcchen.chapter08;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 对于数据不同的两条流合并
 *
 * @author: ceek
 * @create: 2023/3/8 15:49
 */
public class ConnectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Integer> stream1 = env.fromElements(1,2,3);
        DataStream<Long> stream2 = env.fromElements(1L,2L,3L);

        ConnectedStreams<Integer, Long> connectedStreams = stream1.connect(stream2);

        connectedStreams.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "Integer的输出：" + value;
            }

            @Override
            public String map2(Long value) throws Exception {
                return "Long的输出：" + value;
            }
        }).print();

        env.execute();
    }
}
