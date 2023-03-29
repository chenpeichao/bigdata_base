package org.pcchen;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 通过tableApi方式访问mysql的binlog日志
 *
 * @author: ceek
 * @create: 2023/3/29 10:50
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        //1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1、开启ck并指定状态后端为FS
        //Flink-CDC 将读取 binlog 的位置信息以状态的方式保存在 CK,如果想要做到断点续传,需要从 Checkpoint 或者 Savepoint 启动程序
        env.enableCheckpointing(5000l); //开启 Checkpoint,每隔 5 秒钟做一次 CK
        env.setStateBackend(new FsStateBackend("hdfs://192.168.1.101:9000/gmall_flink/ck"));//设置状态后端
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);//指定一致性语义
        env.getCheckpointConfig().setCheckpointTimeout(10000l);         //设置ck的超时时间
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);       //设置最大创建ck数
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000l); //设置ck之间间隔时间，表示前一个结束到后一个开始之间的时间

        //设置访问 HDFS 的用户名
        System.setProperty("HADOOP_USER_NAME", "root");

        //2、通过flinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("192.168.1.101")
                .port(3306)
                .username("root")
                .password("root_123")
                .databaseList("gmall_flink")
                .tableList("gmall_flink.base_trademark")
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        //3、构建数据流，并打印
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);
        mysqlDS.print();

        //4、运行
        env.execute("FlinkCDC");
    }
}
