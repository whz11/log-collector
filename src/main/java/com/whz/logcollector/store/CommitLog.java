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
    private final FlushCommitLogService flushService;
    private final CommitCommitLogService commitService;

    public CommitLog(final DefaultLogStore defaultLogStore) {
        this.mappedFileQueue = new MappedFileQueue(defaultLogStore.getLogStoreConfig().getStorePathCommitLog(),
                defaultLogStore.getLogStoreConfig().getCommitLogSize(), defaultLogStore.getMappedFileFactory());
        this.defaultLogStore = defaultLogStore;
        //异步刷盘服务
        this.flushService = new FlushCommitLogService(defaultLogStore, mappedFileQueue);
        //定时提交服务
        this.commitService = new CommitCommitLogService(defaultLogStore, mappedFileQueue);
    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
        this.flushService.start();
        this.commitService.start();

    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public void shutdown() {
        this.flushService.shutdown();
        this.commitService.shutdown();
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public CompletableFuture<AsyncLogResult> putLog(final LogInner logInner) {

        //获取最后一个mappedFile（可用于写入的文件）
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        AsyncLogResult result;
        putMessageLock.lock();
        try {
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(true);
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

        return CompletableFuture.completedFuture(result);
    }

}
