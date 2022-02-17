package com.whz.logcollector.store;

import com.whz.logcollector.store.config.StorePathConfigHelper;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
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
    // End of file empty MAGIC CODE cbd43194
    protected final static int BLANK_MAGIC_CODE = -875286124;
    // Message's MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = -626843481;

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
        String appName = null;
        try {

            // 1 log total size
            int totalSize = logBlock.getInt();
            // 2 magic code
            int magicCode = logBlock.getInt();
            switch (magicCode) {
                case MESSAGE_MAGIC_CODE:
                    break;
                case BLANK_MAGIC_CODE:
                    return new FetchLogResult(0, true);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new FetchLogResult(-1, false);
            }
            byte[] temp = new byte[totalSize];
            // 3 appNameLength
            int appNameLength = logBlock.getInt();
            // 4 appName
            if (appNameLength > 0) {
                logBlock.get(temp, 0, appNameLength);
                appName = new String(temp, 0, appNameLength, StandardCharsets.UTF_8);
            }
            // 5 bodyLength
            int bodyLength = logBlock.getInt();
            String logContent = null;
            // 6 body
            if (bodyLength > 0) {
                logBlock.get(temp, 0, bodyLength);
                logContent = new String(temp, 0, bodyLength, StandardCharsets.UTF_8);
            }

            int readLength = 4 //log total size
                    + 4//magic code
                    + 4 + appNameLength //appNameLength and appName
                    + 4 + bodyLength //bodyLength and body
                    ;
            ;
            if (totalSize != readLength) {
                log.error(
                        "[BUG]read total count not equals total size. totalSize={}, readLength={}, bodyLength={}, appNameLength={}",
                        totalSize, readLength, bodyLength, appNameLength);
                return new FetchLogResult(totalSize, false);
            }
            return new FetchLogResult(appName, logContent, bodyLength, totalSize, true);
        } catch (Exception e) {
            log.error("[BUG]fetchLog error. appName={}", appName, e);
            return new FetchLogResult(0, true);
        }
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

    public void recoverNormally() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Began to recover from the last third file
            int index = mappedFiles.size() - 3;
            if (index < 0) {
                index = 0;
            }

            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                FetchLogResult fetchLogResult = fetchLog(byteBuffer);
                int size = fetchLogResult.getTotalSize();
                // Normal data
                if (fetchLogResult.isSuccess() && size > 0) {
                    mappedFileOffset += size;
                }
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                else if (fetchLogResult.isSuccess() && size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
                // Intermediate file read error
                else if (!fetchLogResult.isSuccess()) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

        } else {
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
        }
    }
}
