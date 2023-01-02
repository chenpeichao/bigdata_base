package org.pcchen.chapter05;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * 自定义并行读取source
 *
 * @author: ceek
 * @create: 2023/1/2 14:17
 */
public class SourceCustomParallel implements ParallelSourceFunction<Integer> {
    private boolean running = true;
    private Random random = new Random();

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(random.nextInt());
            Thread.sleep(300l);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
