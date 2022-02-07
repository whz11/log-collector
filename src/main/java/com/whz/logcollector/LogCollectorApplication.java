package com.whz.logcollector;

import com.whz.logcollector.store.DefaultLogStore;
import com.whz.logcollector.store.LogInner;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.util.buf.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

import static com.whz.logcollector.FileManager.*;

@SpringBootApplication
@RestController
@Slf4j
@EnableAsync
public class LogCollectorApplication extends SpringBootServletInitializer {


    @Autowired
    private LogCollectorService logCollectorService;
    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    @Value("${store.yaml}")
    private String yamlName;
    private static DefaultLogStore logStore;

    private static final LongAdder count = new LongAdder();

    private static final ScheduledExecutorService SCHEDULE = Executors.newSingleThreadScheduledExecutor();
    private static final List<Integer> OPS_LIST = new ArrayList<>();

    public static void main(String[] args) {
        SpringApplication.run(LogCollectorApplication.class, args);


    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
        return builder.sources(LogCollectorApplication.class);
    }

    @PostConstruct
    public void start() {
        logStore = new DefaultLogStore(yamlName);
        boolean load = logStore.load();
        logStore.start();
        SCHEDULE.scheduleAtFixedRate(() -> {
            OPS_LIST.add(count.intValue());
            count.reset();
            if (OPS_LIST.size() >= 10) {
                log.info(" 前10分钟，每分钟的平均日志吞吐量:{}", OPS_LIST);
                OPS_LIST.clear();
            }
        }, 0, 1, TimeUnit.MINUTES);
    }

    @PreDestroy
    public void destroy() {
        logStore.shutdown();
        SCHEDULE.shutdown();
    }


    @PostMapping("log/{app}")
    public void log(HttpServletRequest request, @PathVariable String app) throws IOException {
        BufferedReader bufferedReader = request.getReader();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            if (line.isEmpty()) {
                continue;
            }
            count.increment();
            logStore.acceptAsync(new LogInner(app, line));
        }
    }

    @GetMapping("info")
    public Map<String, Object> info() {
        ThreadPoolExecutor executor = threadPoolTaskExecutor.getThreadPoolExecutor();
        Map<String, Object> infoMap = new HashMap<>();
        infoMap.put("任务队列任务数量: ", executor.getQueue().size());
        infoMap.put("正在执行的线程数: ", executor.getActiveCount());
        infoMap.put("完成执行的任务总数: ", executor.getCompletedTaskCount());
        infoMap.put("计划执行的任务总数: ", executor.getTaskCount());
        return infoMap;
    }


    @GetMapping("merge")
    public void merge(String appName) {
        logCollectorService.mergeFile(appName);
    }
}
