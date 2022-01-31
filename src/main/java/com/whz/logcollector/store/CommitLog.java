package com.whz.logcollector.store;

import com.whz.logcollector.store.config.StorePathConfigHelper;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
    private final DefaultLogStore defaultLogStore;
    private final FlushCommitLogService flushService;
    private final CommitCommitLogService commitService;
    private final FetchLogService fetchLogService;

    public CommitLog(final DefaultLogStore defaultLogStore) {
        this.mappedFileQueue = new MappedFileQueue(StorePathConfigHelper.getStorePathCommitLog(defaultLogStore.getLogStoreConfig().getStorePathRootDir()),
                defaultLogStore.getLogStoreConfig().getCommitLogSize(), defaultLogStore.getMappedFileFactory());
        this.defaultLogStore = defaultLogStore;
        //异步刷盘服务
        this.flushService = new FlushCommitLogService(defaultLogStore, mappedFileQueue);
        //定时提交服务
        this.commitService = new CommitCommitLogService(defaultLogStore, mappedFileQueue);
        //日志读取服务
        this.fetchLogService = new FetchLogService(defaultLogStore, this);
    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
        this.flushService.start();
        this.commitService.start();
        this.fetchLogService.start();
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public void shutdown() {
        this.flushService.shutdown();
        this.commitService.shutdown();
        this.fetchLogService.shutdown();
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultLogStore.getLogStoreConfig().getCommitLogSize();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public MappedFileResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    public MappedFileResult getData(final long offset, final boolean returnFirstOnNotFound) {
        int mappedFileSize = this.defaultLogStore.getLogStoreConfig().getCommitLogSize();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos);
        }

        return null;
    }

    public FetchLogResult fetchLog(ByteBuffer logBlock) {
        try {

            // 1 log total size
            int totalSize = logBlock.getInt();
            byte[] temp = new byte[totalSize];
            // 2 appNameLength
            int appNameLength = logBlock.getInt();
            // 3 appName
            String appName = null;
            if (appNameLength > 0) {
                logBlock.get(temp, 0, appNameLength);
                appName = new String(temp, 0, appNameLength, StandardCharsets.UTF_8);
            }
            // 4 bodyLength
            int bodyLength = logBlock.getInt();
            String logContent = null;
            // 5 body
            if (bodyLength > 0) {
                logBlock.get(temp, 0, bodyLength);
                logContent = new String(temp, 0, bodyLength, StandardCharsets.UTF_8);
            }

            int readLength = 4 //log total size
                    + 4 + appNameLength //appNameLength and appName
                    + 4 + bodyLength //bodyLength and body
                    ;
            ;
            if (totalSize != readLength && totalSize != 0) {
                log.error(
                        "[BUG]read total count not equals  total size. totalSize={}, readLength={}, bodyLength={}, appNameLength={}",
                        totalSize, readLength, bodyLength, appNameLength);
                return new FetchLogResult(totalSize, false);
            }

            return new FetchLogResult(appName, logContent, bodyLength, totalSize, true);
        } catch (Exception e) {
        }

        return new FetchLogResult(-1, false);
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
