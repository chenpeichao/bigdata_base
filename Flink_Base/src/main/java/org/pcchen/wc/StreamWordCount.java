package org.pcchen.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 无界流进行wordcount计算
 *
 * @author: ceek
 * @create: 2022/9/16 19:08
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = streamEnv.socketTextStream(host, port);

        socketTextStream.flatMap((String line, Collector<String> words) -> {
            Arrays.stream(line.split(" ")).forEach(words :: collect);
        }).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG)).keyBy(kv -> kv.f0).sum(1).print();

        streamEnv.execute();
    }
}
