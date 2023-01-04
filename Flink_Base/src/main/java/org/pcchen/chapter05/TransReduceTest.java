package org.pcchen.chapter05;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 规约聚合示例
 *
 * @author: ceek
 * @create: 2023/1/4 10:26
 */
public class TransReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Mary", "./prod?id=4", 5000L),
                new Event("Mary", "./prod?id=3", 3000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Bob", "./info", 4000L),
                new Event("Alice", "./prod?id=1", 3000L)
        );

        //1、简单排序得到每个用户的最新访问数据
//        stream.keyBy(event -> event.getName())
//                .reduce((Event event1, Event event2) -> event1.getTimestamp() > event2.getTimestamp() ? event1:event2)
//                .print("reduce:");

        //2、统计每个用户的访问次数
        stream.map(event -> Tuple2.of(event.getName(), 1l)).returns(Types.TUPLE(Types.STRING, Types.LONG))
                        .keyBy(tuple2 -> tuple2.f0)
                                .reduce((t1, t2) -> {
                                    if(t1.f0.equals(t2.f0)) {
                                        t1.f1 = t1.f1 + t2.f1;
                                        return t1;
                                    } else {
                                        return t2;
                                    }
                                }).print("reduce:");

        //3、统计访问次数最多的用户
        stream.map(event -> Tuple2.of(event.getName(), 1l)) //将Event数据类型转换成元组类型
                .returns(Types.TUPLE(Types.STRING, Types.LONG)) //由于map使用lambda表达式，会进行泛型擦除
                .keyBy(tuple2 -> tuple2.f0) // 使用用户名来进行分组
                .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1))    //将每个用户的访问次数累加
                .keyBy(tuple2 -> true)  //将所有用户整体划分为一组【为每一条数据分配同一个key，将聚合结果发送到一条流中去】
                .reduce((t1, t2) -> t1.f1 > t2.f1 ? t1 : t2)    //将累加器更新为当前最大的pv统计值，然后向下游发送累加器的值
                .print();
        env.execute();
    }
}
