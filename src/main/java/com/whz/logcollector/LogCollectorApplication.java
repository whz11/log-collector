package com.whz.logcollector;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

import static com.whz.logcollector.FileManager.*;

@SpringBootApplication
@RestController
@Slf4j
@EnableAsync
public class LogCollectorApplication {

    @Autowired
    private LogCollectorService logCollectorService;
    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    public static void main(String[] args) throws FileNotFoundException {
        SpringApplication.run(LogCollectorApplication.class, args);
//        String filePath = PACKAGE + "/vis_log_collector-";
//        BufferedReader reader = new BufferedReader(new FileReader(filePath ));
//        BufferedReader reader1 = new BufferedReader(new FileReader(filePath +1));
//        BufferedReader reader2= new BufferedReader(new FileReader(filePath +2));
//        BufferedReader reader3= new BufferedReader(new FileReader(filePath +3));
//        System.out.println(reader.lines().count());
//        System.out.println(reader1.lines().count());
//        System.out.println(reader2.lines().count());
//        System.out.println(reader3.lines().count());
    }


    @PostMapping("log/{app}")
    public void log(HttpServletRequest request, @PathVariable String app) throws IOException {
        BufferedReader bufferedReader = request.getReader();
        String year = LocalDate.now().getYear() + "-";
        String str;
        StringBuilder buffer = new StringBuilder();
        while ((str = bufferedReader.readLine()) != null) {
            if (buffer.length() != 0 && str.startsWith(year)) {
                logCollectorService.acceptAsync(app, buffer.toString());
                buffer.setLength(0);
            }
            buffer.append(str).append("\n");
        }
        if (buffer.length() != 0) {
            logCollectorService.acceptAsync(app, buffer.toString());
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
