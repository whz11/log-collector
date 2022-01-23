package com.whz.logcollector.store;

import com.whz.logcollector.store.config.LogStoreConfig;
import com.whz.logcollector.store.config.StorePathConfigHelper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

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
    private final AllocateMappedFileService allocateMappedFileService;

    public DefaultLogStore() {
        this.logStoreConfig = new LogStoreConfig();
        this.allocateMappedFileService = new AllocateMappedFileService(this);
        this.commitLog = new CommitLog(this);
        this.directByteBufferPool = new DirectByteBufferPool();
        this.directByteBufferPool.init();
    }

    @Override
    public CompletableFuture<Boolean> asyncPut(LogInner logInner) {
        return this.commitLog.putMessage(logInner).thenApply(result -> result.getStatus() == AppendMessageStatus.OK);
    }

    @Override
    public boolean load() {
        boolean result = true;

        try {

            //加载mappedFile到mappedFileQueue
            result = result && this.commitLog.load();


            if (result) {

            }
        } catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }


        return result;
    }

    /**
     * @throws Exception
     */
    @Override
    public void start() throws Exception {


        this.commitLog.start();
        this.allocateMappedFileService.start();

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

            this.allocateMappedFileService.shutdown();
            this.commitLog.shutdown();

        }

        this.directByteBufferPool.destroy();

    }

    @Override
    public void destroy() {
        this.commitLog.destroy();
    }
}