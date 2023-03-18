package org.pcchen.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.pcchen.chapter05.ClickSource;
import org.pcchen.chapter05.Event;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 算子状态liststate中对于sink后数据检查点恢复
 *
 * @author: ceek
 * @create: 2023/3/18 19:41
 */
public class BufferingSinkExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }));

        stream.print("input:");

        stream.addSink(new BufferingSink(10l));

        env.execute();
    }

    /**
     * 用算子状态中的ListState模拟进行数据sink时数据恢复
     */
    public static class BufferingSink implements CheckpointedFunction, SinkFunction<Event> {
        //定义一个数据批量sink的阈值
        private Long threshold;
        //定义数据批量sink的list集合
        private List<Event> bufferedElements;
        //定义进行数据检查点中保存的数据集合，进行数据恢复时使用
        private ListState<Event> checkpointedState;

        public BufferingSink(Long threshold) {
            this.threshold = threshold;
            bufferedElements = new ArrayList<Event>();
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            //读取列表中的数据进行输出-控制台打印
            bufferedElements.add(value);
            if (bufferedElements.size() == threshold) {
                for (Event element : bufferedElements) {
                    // 输出到外部系统，这里用控制台打印模拟
                    System.out.println(element);
                }
                System.out.println("==========输出完毕=========");
                bufferedElements.clear();
            }
        }


        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            // 把当前局部变量中的所有元素写入到检查点中
            for (Event element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            checkpointedState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Event>("buffered-elements", Event.class));
            //将检查点中恢复的数据放入集合中
            // 如果是从故障中恢复，就将ListState中的所有元素添加到局部变量中
            if (context.isRestored()) {
                for (Event element : checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }
        }
    }
}
