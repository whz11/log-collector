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
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
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
    @Value("${logcollector.store.rootdir}")
    private String rootDir;

    private static DefaultLogStore logStore;

    private static final LongAdder count = new LongAdder();

    public static void main(String[] args) {
        SpringApplication.run(LogCollectorApplication.class, args);


        //统计qps
//        Timer timer = new Timer();
//        timer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                log.info("qps:{}", count.longValue());
//                count.reset();
//            }
//        }, 5000, 1000);
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
        return builder.sources(LogCollectorApplication.class);
    }

    @PostConstruct
    public void start() {
        logStore = new DefaultLogStore(rootDir);
        boolean load = logStore.load();
        logStore.start();

    }

    @PreDestroy
    public void destroy() {
        logStore.shutdown();
    }


    @PostMapping("log/{app}")
    public void log(HttpServletRequest request, @PathVariable String app) throws IOException {
        BufferedReader bufferedReader = request.getReader();
        String line;
        while ((line = bufferedReader.readLine()) != null ) {
            if(line.isEmpty()){
                continue;
            }
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
