package com.whz.logcollector.store;

import lombok.extern.slf4j.Slf4j;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
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

    public CommitLog(final DefaultLogStore defaultLogStore) {
        this.mappedFileQueue = new MappedFileQueue(defaultLogStore.getLogStoreConfig().getStorePathCommitLog(),
                defaultLogStore.getLogStoreConfig().getCommitLogSize(), defaultLogStore.getAllocateMappedFileService());
        this.defaultLogStore = defaultLogStore;

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

    public CompletableFuture<AppendMessageResult> putMessage(final LogInner logInner) {

        //获取最后一个mappedFile（可用于写入的文件）
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        AppendMessageResult result = null;
        putMessageLock.lock();
        try {

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0);
            }
            if (null == mappedFile) {
                return CompletableFuture.completedFuture(new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR));
            }
            result = mappedFile.appendMessagesInner(logInner);
            if (result.getStatus() == AppendMessageStatus.END_OF_FILE) {
                // Create a new file, re-write the message
                mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                if (null == mappedFile) {
                    return CompletableFuture.completedFuture(new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR));
                }
                return putMessage(logInner);
            }

        } finally {
            putMessageLock.unlock();
        }
        return CompletableFuture.completedFuture(result);

//
//        CompletableFuture<PutMessageStatus> flushResultFuture = submitFlushRequest(result, msg);
//        CompletableFuture<PutMessageStatus> replicaResultFuture = submitReplicaRequest(result, msg);
//        return flushResultFuture.thenCombine(replicaResultFuture, (flushStatus, replicaStatus) -> {
//            if (flushStatus != PutMessageStatus.PUT_OK) {
//                putMessageResult.setPutMessageStatus(flushStatus);
//            }
//            if (replicaStatus != PutMessageStatus.PUT_OK) {
//                putMessageResult.setPutMessageStatus(replicaStatus);
//                if (replicaStatus == PutMessageStatus.FLUSH_SLAVE_TIMEOUT) {
//                    log.error("do sync transfer other node, wait return, but failed, topic: {} tags: {} client address: {}",
//                            msg.getTopic(), msg.getTags(), msg.getBornHostNameString());
//                }
//            }
//            return putMessageResult;
//        });
    }
}
