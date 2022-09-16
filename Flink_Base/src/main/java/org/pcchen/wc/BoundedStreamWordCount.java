package org.pcchen.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 有界流读取文本文件wordcount
 *
 * @author: ceek
 * @create: 2022/9/16 18:06
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        //1. 创建流式执行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. 读取文件
        DataStreamSource<String> lineDSSource = streamEnv.readTextFile("E:\\code\\github\\bigdata_base\\Flink_Base\\input\\word.txt");

        // 3. 转换数据格式
//        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneStream = lineDSSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
//            String[] words = line.split(" ");
//            for (String word : words) {
//                out.collect(Tuple2.of(word, 1L));
//            }
//        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneStream = lineDSSource.flatMap((String line, Collector<String> words) -> {
            Arrays.stream(line.split(" ")).forEach(words::collect);
        }).returns(Types.STRING)
                .map(word -> new Tuple2<String, Long>(word, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOneStream.keyBy(t -> t.f0);
//        KeyedStream<Tuple2<String, Long>, String> result = wordAndOneStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
//            @Override
//            public String getKey(Tuple2<String, Long> value) throws Exception {
//                return value.f0;
//            }
//        });

        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);

        result.print();

        streamEnv.execute();
    }
}
