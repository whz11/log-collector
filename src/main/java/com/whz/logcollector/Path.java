package com.whz.logcollector;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static com.whz.logcollector.FileManager.*;

/**
 * @author whz
 * @date 2022/1/11 21:03
 **/
public class Path {
    List<String> childList;

    public Path() {
        childList = new ArrayList<>();
    }

    public Path append(String child) {
        childList.add(child);
        return this;
    }

    public String build() {
        return String.join("/", childList);
    }

    public static String collector(String appName) {
        int bound = 2;
        //1-bound
        int factor = new Random().nextInt(bound) + 1;
        String fileName = Thread.currentThread().getId() + String.valueOf(factor) + ".log";
        return new Path()
                .append(R)
                .append(PathEnum.COLLECTOR.name())
                .append(appName)
                .append(getSurvivor(appName))
                .append(fileName)
                .build();
    }

    public static String log(String appName) {
        return new Path()
                .append(R)
                .append(PathEnum.LOG.name())
                .append(appName)
                .append(LocalDate.now() + ".log")
                .build();
    }

    public static String app(String appFile) {
        return new Path()
                .append(R)
                .append(PathEnum.APP.name())
                .append(appFile)
                .build();
    }

}
