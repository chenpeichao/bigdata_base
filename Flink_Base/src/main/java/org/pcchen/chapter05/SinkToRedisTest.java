package org.pcchen.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 数据写入redis示例
 *
 * @author: ceek
 * @create: 2023/1/4 18:04
 */
public class SinkToRedisTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> clickSourceStream = env.addSource(new ClickSource());

        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig
                .Builder()
                .setHost("192.168.1.101")
                .setPort(6379)
                .build();
        clickSourceStream.addSink(new RedisSink(jedisPoolConfig, new MyRedisMapper()));

        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<Event> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            //hash结构：clicks -> key:name;field:event.toString
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
            //string结构：key:name;field:event.toString
//            return new RedisCommandDescription(RedisCommand.SET);
//            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        @Override
        public String getKeyFromData(Event data) {
            return data.getName();
        }

        @Override
        public String getValueFromData(Event data) {
            return data.toString();
        }
    }
}
