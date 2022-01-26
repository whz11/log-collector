package com.whz.logcollector.store;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author whz
 * @date 2022/1/17 16:40
 **/
@Slf4j
public class CommitLog {
    private ReentrantLock putMessageLock = new ReentrantLock();
    protected MappedFileQueue mappedFileQueue;
    private DefaultLogStore defaultLogStore;
    private final FlushCommitLogService commitLogService;

    public CommitLog(final DefaultLogStore defaultLogStore) {
        this.mappedFileQueue = new MappedFileQueue(defaultLogStore.getLogStoreConfig().getStorePathCommitLog(),
                defaultLogStore.getLogStoreConfig().getCommitLogSize(), defaultLogStore.getMappedFileFactory());
        this.defaultLogStore = defaultLogStore;
        this.commitLogService=new FlushCommitLogService(defaultLogStore,mappedFileQueue);

    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
//        this.flushCommitLogService.start();
//
//        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
//            this.commitLogService.start();
//        }
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public void shutdown() {
//        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
//            this.commitLogService.shutdown();
//        }
//
//        this.flushCommitLogService.shutdown();
    }

    public CompletableFuture<AsyncLogResult> putLog(final LogInner logInner) {

        //获取最后一个mappedFile（可用于写入的文件）
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        AsyncLogResult result;
        putMessageLock.lock();
        try {
            if (null == mappedFile || mappedFile.isFull()) {
                long t1 = System.currentTimeMillis();
                mappedFile = this.mappedFileQueue.getLastMappedFile(true);
                log.info("time:{}ms", System.currentTimeMillis() - t1);
            }
            if (null == mappedFile) {
                return CompletableFuture.completedFuture(new AsyncLogResult(AsyncLogStatus.UNKNOWN_ERROR));
            }
            result = mappedFile.appendLogInner(logInner);
            if (result.getStatus() == AsyncLogStatus.END_OF_FILE) {
                // Create a new file, re-write the message
                mappedFile = this.mappedFileQueue.getLastMappedFile(true);
                if (null == mappedFile) {
                    return CompletableFuture.completedFuture(new AsyncLogResult(AsyncLogStatus.UNKNOWN_ERROR));
                }
                return putLog(logInner);
            }

        } finally {
            putMessageLock.unlock();
        }
        commitLogService.wakeup();

        return CompletableFuture.completedFuture(result);
    }

}
