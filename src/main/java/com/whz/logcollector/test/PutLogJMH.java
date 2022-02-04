package com.whz.logcollector.test;

import com.whz.logcollector.store.DefaultLogStore;
import com.whz.logcollector.store.LogInner;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author whz
 * @date 2022/2/1 12:47
 **/
@State(Scope.Benchmark)
public class PutLogJMH {
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PutLogJMH.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    private static DefaultLogStore defaultLogStore;
    private static LogInner logInner;

    @Param({"1"})
    private int strCount;
    @Param({"1"})
    private int count;

    @Setup
    public void prepare() {
        defaultLogStore = new DefaultLogStore();
        defaultLogStore.load();
        defaultLogStore.start();
        logInner = new LogInner();
        logInner.setAppName("jmh");
        StringBuilder body = new StringBuilder("这是jmh性能测试的body");
        for (int i = 0; i < strCount; i++) {
            body.append("这是jmh性能测试的body");
        }
        logInner.setBody(body.toString());

    }
    @TearDown
    public void destroy(){
        defaultLogStore.shutdown();
    }


    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Measurement(iterations = 1)
    @Warmup(iterations = 1)
    @Timeout(time = 10)
    public void putLog() {
            defaultLogStore.acceptAsync(logInner);
    }
}
