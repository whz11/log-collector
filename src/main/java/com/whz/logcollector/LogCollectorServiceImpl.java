package com.whz.logcollector;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.whz.logcollector.FileManager.*;

/**
 * @author whz
 * @date 2022/1/8 16:52
 **/
@Service
public class LogCollectorServiceImpl implements LogCollectorService {
    @Override
    @Async
    public void acceptAsync(String appName, String logMessage) {

        String path = Path.collector(appName);
        write(path, bw -> {
            try {
                bw.write(logMessage);
                bw.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        register(appName);
    }

    @Override
    public void mergeFile(String appName) {
        String dirPath = new Path()
                .append(R)
                .append(PathEnum.COLLECTOR.name())
                .append(appName)
                .append(getSurvivor(appName)).build();
        //切换from to区，生成快照
        setSurvivor(appName);
        try {
            Thread.sleep(50);
            List<BufferedReader> bufferedReaderList = new ArrayList<>();
            for (String path : getAllFile(dirPath)) {
                BufferedReader reader = new BufferedReader(new FileReader(path));
                bufferedReaderList.add(reader);
            }
            FailedTree<String, BufferedReader> failedTree = new FailedTree<>(bufferedReaderList,
                    (t1, t2) -> getLogTimestamp(t1) > getLogTimestamp(t2));
            String logPath = Path.log(appName);
            FileManager.write(logPath, bw -> {
                String winner;
                try {
                    while ((winner = failedTree.getWinner()) != null) {
                        bw.write(winner);
                        bw.newLine();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (FileNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private long getLogTimestamp(String singleLog) {
        StringBuilder sb = new StringBuilder();
        char[] charArr = singleLog.toCharArray();
        int magic = 0, length = 23;
        for (int i = 0; i < length; i++) {
            sb.append(charArr[i + magic]);
        }
        DateTimeFormatter ftf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        LocalDateTime parse = LocalDateTime.parse(sb, ftf);
        return LocalDateTime.from(parse).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

}

