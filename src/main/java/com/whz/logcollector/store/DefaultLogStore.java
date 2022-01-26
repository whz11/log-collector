package com.whz.logcollector.store;

import com.whz.logcollector.store.config.LogStoreConfig;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

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
    private final MappedFileFactory mappedFileFactory;

    public DefaultLogStore() {
        this.logStoreConfig = new LogStoreConfig();
        this.mappedFileFactory = new MappedFileFactory(this);
        this.commitLog = new CommitLog(this);
        this.directByteBufferPool = new DirectByteBufferPool(this.logStoreConfig);
        this.directByteBufferPool.init();
    }

    @Override
    public CompletableFuture<Boolean> asyncPut(LogInner logInner) {
        return this.commitLog.putLog(logInner).thenApply(result -> result.getStatus() == AsyncLogStatus.OK);
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

        }

        this.directByteBufferPool.destroy();

    }

    @Override
    public void destroy() {
        this.commitLog.destroy();
    }
}