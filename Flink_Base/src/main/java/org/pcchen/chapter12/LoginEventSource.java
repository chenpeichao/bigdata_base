package org.pcchen.chapter12;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 登录事件产生类
 *
 * @author: ceek
 * @create: 2023/3/28 8:52
 */
public class LoginEventSource implements SourceFunction<LoginEvent> {
    // 声明一个布尔变量，作为控制数据生成的标识位
    private Boolean flag = true;

    @Override
    public void run(SourceContext<LoginEvent> sourceContext) throws Exception {
        Random random = new Random();    // 在指定的数据集中随机选取数据
        String[] userIds = {"user1", "user2"};
        String[] ips = {"192.168.0.1", "192.168.0.2", "192.168.0.3", "192.168.0.4", "192.168.0.5"};

//        while (flag) {
//            sourceContext.collect(new LoginEvent(
//                    userIds[random.nextInt(userIds.length)],
//                    ips[random.nextInt(ips.length)],
//                    "fail",
//                    Calendar.getInstance().getTimeInMillis()
//            ));
//            // 隔1秒生成一个点击事件，方便观测
//            Thread.sleep(1000l);
//        }
        long timeInMillis = Calendar.getInstance().getTimeInMillis();
        sourceContext.collect(new LoginEvent(
                "user1",
                ips[random.nextInt(ips.length)],
                "fail",
                timeInMillis
        ));
        // 隔1秒生成一个点击事件，方便观测
        Thread.sleep(1000l);
        sourceContext.collect(new LoginEvent(
                "user1",
                ips[random.nextInt(ips.length)],
                "fail",
                Calendar.getInstance().getTimeInMillis()
        ));
        // 隔1秒生成一个点击事件，方便观测
        Thread.sleep(1000l);
        sourceContext.collect(new LoginEvent(
                "user1",
                ips[random.nextInt(ips.length)],
                "fail",
                Calendar.getInstance().getTimeInMillis()
        ));
        // 隔1秒生成一个点击事件，方便观测
        Thread.sleep(1000l);sourceContext.collect(new LoginEvent(
                "user1",
                ips[random.nextInt(ips.length)],
                "fail",
                Calendar.getInstance().getTimeInMillis()
        ));
        // 隔1秒生成一个点击事件，方便观测
        Thread.sleep(1000l);sourceContext.collect(new LoginEvent(
                "user1",
                ips[random.nextInt(ips.length)],
                "fail",
                Calendar.getInstance().getTimeInMillis()
        ));
        // 隔1秒生成一个点击事件，方便观测
        Thread.sleep(1000l);sourceContext.collect(new LoginEvent(
                "user2",
                ips[random.nextInt(ips.length)],
                "fail",
                Calendar.getInstance().getTimeInMillis()
        ));
        // 隔1秒生成一个点击事件，方便观测
        Thread.sleep(1000l);
        sourceContext.collect(new LoginEvent(
                "user2",
                ips[random.nextInt(ips.length)],
                "fail",
                Calendar.getInstance().getTimeInMillis()
        ));
        // 隔1秒生成一个点击事件，方便观测
        Thread.sleep(1000l);sourceContext.collect(new LoginEvent(
                "user1",
                ips[random.nextInt(ips.length)],
                "success",
                timeInMillis + 1000
        ));
        // 隔1秒生成一个点击事件，方便观测
        Thread.sleep(1000l);sourceContext.collect(new LoginEvent(
                "user1",
                ips[random.nextInt(ips.length)],
                "fail",
                Calendar.getInstance().getTimeInMillis()
        ));
        // 隔1秒生成一个点击事件，方便观测
        Thread.sleep(1000l);
        sourceContext.collect(new LoginEvent(
                "user2",
                ips[random.nextInt(ips.length)],
                "fail",
                Calendar.getInstance().getTimeInMillis()
        ));
        // 隔1秒生成一个点击事件，方便观测
        Thread.sleep(1000l);
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
