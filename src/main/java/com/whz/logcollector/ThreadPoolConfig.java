package com.whz.logcollector;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author whz
 * @date 2022/1/8 19:09
 **/
@Configuration
public class ThreadPoolConfig {

    /**
     * 核心线程数
     */
    public static final int CORE_POOL_SIZE = 4;
    /**
     * 最大线程数
     */
    public static final int MAX_POOL_SIZE = 4;
    /**
     * 队列数
     */
    private static final int QUEUE_CAPACITY = 1000000;

    @Bean
    public ThreadPoolTaskExecutor threadPoolTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(CORE_POOL_SIZE);
        executor.setMaxPoolSize(MAX_POOL_SIZE);
        executor.setQueueCapacity(QUEUE_CAPACITY);
        executor.setThreadNamePrefix("log_collector-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.initialize();
        return executor;
    }

}
