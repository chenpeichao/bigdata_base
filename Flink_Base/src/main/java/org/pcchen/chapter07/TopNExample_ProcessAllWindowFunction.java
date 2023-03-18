package org.pcchen.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.pcchen.chapter05.ClickSource;
import org.pcchen.chapter05.Event;
import scala.Tuple2;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 * 使用全窗口处理函数处理topN
 *  需求：每隔5秒统计最近10秒的top2的热门url
 * @author: ceek
 * @create: 2023/3/7 17:31
 */
public class TopNExample_ProcessAllWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                );

        stream.print();
        stream      //定义滑动窗口-每5秒滑动大小为10秒的窗口
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                        .process(new ProcessAllWindowFunction<Event, String, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<Event, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                                //定义一个HashMap用来封装整体数据中的url和其数量<url, count>
                                HashMap<String, Long> urlCountHashMap = new HashMap<String, Long>();
                                for(Event event : elements) {
                                    if(urlCountHashMap.containsKey(event.getUrls())) {
                                        urlCountHashMap.put(event.getUrls(), urlCountHashMap.get(event.getUrls()) + 1l);
                                    } else {
                                        urlCountHashMap.put(event.getUrls(), 1l);
                                    }
                                }

                                //将上面的map转换为一个二元组的list进行排序
                                ArrayList<Tuple2<String, Long>> mapList = new ArrayList<Tuple2<String, Long>>();
                                for(String urls : urlCountHashMap.keySet()) {
                                    mapList.add(new Tuple2<String, Long>(urls, urlCountHashMap.get(urls)));
                                }

                                mapList.sort(new Comparator<Tuple2<String, Long>>() {
                                    @Override
                                    public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                                        return o2._2.intValue() - o1._2.intValue();
                                    }
                                });

                                // 取排序后的前两名，构建输出结果
                                StringBuilder result = new StringBuilder();
                                result.append("========================================\n");
                                for (int i = 0; i < 2; i++) {
                                    Tuple2<String, Long> temp = mapList.get(i);
                                    String info = "浏览量No." + (i + 1) +
                                            " url：" + temp._1+
                                            " 浏览量：" + temp._2 +
                                            " 窗口起始时间：" + new Timestamp(context.window().getStart()) +
                                            " 窗口结束时间：" + new Timestamp(context.window().getEnd()) + "\n";

                                    result.append(info);
                                }
                                result.append("========================================\n");
                                out.collect(result.toString());
                            }
                        }).print();
        /**
         * 方式二：不通过process，直接通过aggregate的api内部公国agg的function和process的function计算
         */
//        // 只需要url就可以统计数量，所以转换成String直接开窗统计
//        SingleOutputStreamOperator result = stream
//                .map(new MapFunction<Event, String>() {
//                    @Override
//                    public String map(Event value) throws Exception {
//                        return value.getUrls();
//                    }
//                })
//                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))    // 开滑动窗口
//                .aggregate(new UrlHashMapCountAgg(), new UrlAllWindowResult());
//
//        result.print();

        env.execute();
    }

    public static class UrlHashMapCountAgg implements AggregateFunction<String, HashMap<String, Long>, HashMap<String, Long>> {

        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<String, Long>();
        }

        @Override
        public HashMap<String, Long> add(String url, HashMap<String, Long> urlCountHashMap) {
            if(urlCountHashMap.containsKey(url)) {
                urlCountHashMap.put(url, urlCountHashMap.get(url) + 1l);
            } else {
                urlCountHashMap.put(url, 1l);
            }
            return urlCountHashMap;
        }

        @Override
        public HashMap<String, Long> getResult(HashMap<String, Long> accumulator) {
            return accumulator;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> a, HashMap<String, Long> b) {
            return null;
        }
    }

    public static class UrlAllWindowResult extends ProcessAllWindowFunction<HashMap<String, Long>, String, TimeWindow> {

        @Override
        public void process(ProcessAllWindowFunction<HashMap<String, Long>, String, TimeWindow>.Context context, Iterable<HashMap<String, Long>> elements, Collector<String> out) throws Exception {
            HashMap<String, Long> hashMap = elements.iterator().next();

            ArrayList<Tuple2<String, Long>> arrayList = new ArrayList<Tuple2<String, Long>>();
            for(String url : hashMap.keySet()) {
                arrayList.add(Tuple2.apply(url, hashMap.get(url)));
            }

            arrayList.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return o2._2.intValue() - o1._2.intValue();
                }
            });

            // 取排序后的前两名，构建输出结果
            StringBuilder result = new StringBuilder();
            result.append("========================================\n");
            for (int i = 0; i < 2; i++) {
                Tuple2<String, Long> temp = arrayList.get(i);
                String info = "浏览量No." + (i + 1) +
                        " url：" + temp._1 +
                        " 浏览量：" + temp._2 +
                        " 窗口结束时间：" + new Timestamp(context.window().getEnd()) + "\n";

                result.append(info);
            }
            result.append("========================================\n");
            out.collect(result.toString());
        }
    }
}
