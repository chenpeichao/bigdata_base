package org.pcchen.chapter10;

import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 检查点的配置相关
 *
 * @author: ceek
 * @create: 2023/3/18 23:26
 */
public class CheckPointExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(1000l); //设置检查点的时间间隔为1秒

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //设置检查点的相关信息
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://"));
        checkpointConfig.setCheckpointTimeout(60000l);  //检查点的失效时间
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);  //
        checkpointConfig.setMinPauseBetweenCheckpoints(500l);   //设置检查点完成后到下一个检查点触发的最小时间间隔；用来提高数据处理性能
        //....

        env.execute();
    }
}
