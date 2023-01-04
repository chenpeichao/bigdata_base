package org.pcchen.redis;

import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * 单机redis测试类
 *
 * @author: ceek
 * @create: 2023/1/4 18:23
 */
public class JedisTest {
    public static void main(String[] args) {
        Jedis jedis=new Jedis("192.168.1.101",6379);
//        jedis.set("k1","1");
//        jedis.setnx("k1","4");
//        jedis.setnx("k2","2");
//        jedis.mset("k3","3","k4","4");
//        List<String> mget = jedis.mget("k1", "k2", "k3", "k4");
//        System.out.println(mget);
//        System.out.println(jedis.get("username"));

        Map<String, String> clicks = jedis.hgetAll("clicks");
        Set<Map.Entry<String, String>> entries = clicks.entrySet();
        for(Iterator<Map.Entry<String, String>> iterator = entries.iterator(); iterator.hasNext();) {
            Map.Entry<String, String> next = iterator.next();
            System.out.println(next.getKey() + "=>" + next.getValue());
        }
//        jedis.flushAll();
    }
}
