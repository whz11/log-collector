package com.whz.logcollector.store;

import com.whz.logcollector.store.config.LogStoreConfig;
import com.whz.logcollector.store.config.StorePathConfigHelper;
import com.whz.logcollector.store.util.YamlUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author whz
 * @date 2022/1/16 11:34
 **/
@Slf4j
@Data
public class DefaultLogStore implements LogStore {
    private final CommitLog commitLog;
    private final DirectByteBufferPool directByteBufferPool;
    private volatile boolean shutdown = true;
    private final LogStoreConfig logStoreConfig;
    private final MappedFileFactory mappedFileFactory;
    private StoreCheckpoint storeCheckpoint;
    private final ConcurrentMap<String, AppLogFile> appLogFileTable;

    public DefaultLogStore(String yamlName) {
        this.logStoreConfig = YamlUtil.read(yamlName, LogStoreConfig.class);
        this.mappedFileFactory = new MappedFileFactory(this);
        this.commitLog = new CommitLog(this);
        this.directByteBufferPool = new DirectByteBufferPool(this.logStoreConfig);
        this.directByteBufferPool.init();
        this.appLogFileTable = new ConcurrentHashMap<>(32);
    }

    public DefaultLogStore() {
        this.logStoreConfig = new LogStoreConfig();
        this.mappedFileFactory = new MappedFileFactory(this);
        this.commitLog = new CommitLog(this);
        this.directByteBufferPool = new DirectByteBufferPool(this.logStoreConfig);
        this.directByteBufferPool.init();
        this.appLogFileTable = new ConcurrentHashMap<>(32);
    }

    @Override
    public CompletableFuture<Boolean> acceptAsync(LogInner logInner) {
        return this.commitLog.putLog(logInner).thenApply(result -> result.getStatus() == AsyncLogStatus.OK);
    }

    public void doDispatch(FetchLogResult fetchLogResult) {
        AppLogFile appLogFile = findAppLogFile(fetchLogResult.getApp());
        appLogFile.write(fetchLogResult.getBody(), fetchLogResult.getBodySize());
    }

    public AppLogFile findAppLogFile(String app) {
        return appLogFileTable.compute(app, (k, v) -> {
            try {
                LocalDate now = LocalDate.now();
                if (null == v || !v.getFileDate().isEqual(now)) {
                    if (v != null) {
                        v.shutdown();
                    }
                    v = new AppLogFile(this, now, app);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            return v;
        });


    }

    private boolean loadAppLogFiles() throws FileNotFoundException {
//        File appLogRoot = new File(StorePathConfigHelper.getStorePathAppLog(this.logStoreConfig.getStorePathRootDir()));
//        File[] appDirList = appLogRoot.listFiles();
//        if (appDirList != null) {
//
//            for (File appDir : appDirList) {
//                if (!appDir.isDirectory()) {
//                    continue;
//                }
//                String app = appDir.getName();
//                File[] logByDateList = appDir.listFiles();
//                if (logByDateList != null) {
//                    for (File logByDate : logByDateList) {
//                        String curDate = LocalDate.now().toString();
//                        if (curDate.equals(logByDate.getName())) {
//                            log.info("load app:{},path:{}", app, logByDate.getPath());
//                            AppLogFile curFile = new AppLogFile(this, app);
//                            this.appLogFileTable.put(app, curFile);
//                            break;
//                        }
//                    }
//                }
//            }
//        }

        log.info("load logics queue all over, OK");

        return true;
    }

    @Override
    public boolean load() {
        boolean result;

        try {
            result = this.commitLog.load() && this.loadAppLogFiles();
            if (result) {
                this.storeCheckpoint = new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.getLogStoreConfig().getStorePathRootDir()));
            }
        } catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }


        return result;
    }

    /**
     *
     */
    @Override
    public void start() {


        this.commitLog.start();

        this.shutdown = false;
    }

    @Override
    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            this.mappedFileFactory.shutdown();
            this.commitLog.shutdown();
            this.storeCheckpoint.shutdown();
            for (AppLogFile appLogFile : appLogFileTable.values()) {
                appLogFile.shutdown();
            }

        }

        this.directByteBufferPool.destroy();

    }

    @Override
    public long flush() {
        return this.commitLog.flush();
    }

    @Override
    public void destroy() {
        this.commitLog.destroy();
    }
}