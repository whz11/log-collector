package com.whz.logcollector;

import java.io.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * 日志收集器的文件系统
 *
 * @author whz
 * @date 2022/1/9 22:05
 **/
public class FileManager {
    public static final String R = "fileSystem";
    public static final String L = "/";

    /**
     * 注册到日志收集器的服务
     */
    private static Set<String> registers = new HashSet<>();

    static {
        read(Path.app("appList.log"), reader -> registers = reader.lines().collect(Collectors.toSet()));
    }

    /**
     * 幸存区记录
     * app日志采集器有两个存储区，每次只写入其中一个，另一个用来保存快照，方便合并
     */
    private static volatile Map<String, Survivor> SURVIVORS = new HashMap<>();

    public static String getSurvivor(String appName) {
        return SURVIVORS.computeIfAbsent(appName, v -> new Survivor()).getArea() ? "from" : "to";
    }

    public static void setSurvivor(String appName) {
        SURVIVORS.computeIfAbsent(appName, v -> new Survivor()).changeArea();
    }

    public static void register(String app) {
        if (!registers.contains(app)) {
            registers.add(app);
            write(Path.app("appList.log"), bw -> {
                try {
                    bw.write(app);
                    bw.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }


    public static List<String> getAllFile(String directoryPath) {
        List<String> list = new ArrayList<>();
        File baseFile = new File(directoryPath);
        if (baseFile.isFile() || !baseFile.exists()) {
            return list;
        }
        File[] files = baseFile.listFiles();
        if (files == null) {
            return list;
        }
        for (File file : files) {
            list.add(directoryPath + L + file.getName());
        }
        return list;
    }

    public static void read(String path, Consumer<BufferedReader> consumer) {
        mkdir(path);
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            consumer.accept(reader);
        } catch (IOException e) {
            System.err.format("IOException: %s%n", e);
        }
    }

    public static File mkdir(String path) {
        File f = new File(path);
        if (!f.getParentFile().exists()) {
            f.getParentFile().mkdirs();
        }
        return f;
    }

    public static void write(String path, Consumer<BufferedWriter> consumer) {
        mkdir(path);
        try (FileWriter filewriter = new FileWriter(path, true);
             BufferedWriter writer = new BufferedWriter(filewriter)) {
            consumer.accept(writer);
        } catch (IOException e) {
            System.err.format("IOException: %s%n", e);
        }
    }
}
