package org.pcchen.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * kafka从各种数据源中获取数据
 *
 * @author: ceek
 * @create: 2023/1/2 13:31
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1、从文件中读取数据
        DataStreamSource<String> streamText = env.readTextFile("E:\\code\\github\\bigdata_base\\Flink_Base\\input\\word.txt");

        //2、从socket中获取数据
        DataStreamSource<String> streamSocket = env.socketTextStream("192.168.1.101", 7777);

        //3、从元素中读取数据
        DataStreamSource<Event> elementStream = env.fromElements(
                new Event("zhangsan", "./home", 1000l),
                new Event("lisi", "./cart", 1000l)
        );

        //4、从用户自定义source中获取数据
        DataStreamSource<Event> customSourceStream = env.addSource(new ClickSource());

        //5、从用户自定义并行source中获取数据
        DataStreamSource<Integer> customParallelStream = env.addSource(new SourceCustomParallel()).setParallelism(3);

//        streamText.print();
//        streamSocket.print();
//        elementStream.print();
//        customSourceStream.print("SourceCustom");
        customParallelStream.print("customParallelStream");

        env.execute();
    }
}
