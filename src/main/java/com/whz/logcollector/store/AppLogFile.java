package com.whz.logcollector.store;

import com.whz.logcollector.store.config.StorePathConfigHelper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author whz
 * @date 2022/1/18 21:20
 **/
@Slf4j
@Data
public class AppLogFile {
    private final DefaultLogStore defaultLogStore;
    private final FileChannel fileChannel;
    private LocalDate fileDate;
    private String app;
    private File file;

    public AppLogFile(final DefaultLogStore defaultLogStore, LocalDate fileDate, String app) throws FileNotFoundException {
        this.defaultLogStore = defaultLogStore;
        this.fileDate = fileDate;
        this.app = app;
        file = new File(StorePathConfigHelper.getAppLogFile(defaultLogStore.getLogStoreConfig().getStorePathRootDir(), app, fileDate));
        MappedFile.ensureDirOK(file.getParent());
        this.fileChannel = new FileOutputStream(file, true).getChannel();
    }

    public void shutdown() {
        try {
            this.fileChannel.close();
        } catch (IOException e) {
            log.error("Failed to properly close the channel", e);
        }
    }

    public void write(String line, Integer bufferSize) {
        ByteBuffer buf = ByteBuffer.allocate(bufferSize + 10);
        try {
            line = line + "\n";
            buf.put(line.getBytes());
            buf.flip();   // 切换为读模式
            while (buf.hasRemaining()) {
                fileChannel.write(buf);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
