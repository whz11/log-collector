package com.whz.logcollector.store;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * @author whz
 * @date 2022/1/27 19:53
 **/
@Slf4j
@Data
public class FetchLogService extends ServiceThread {
    public static volatile long progressFromOffset = 0;
    private final DefaultLogStore defaultLogStore;
    private final CommitLog commitLog;

    public FetchLogService(DefaultLogStore defaultLogStore, CommitLog commitLog) {
        this.defaultLogStore = defaultLogStore;
        this.commitLog = commitLog;
    }

    @Override
    public void shutdown() {
        defaultLogStore.getStoreCheckpoint().setProgressFromOffset(progressFromOffset);
        super.shutdown();
    }

    public long behind() {
        return commitLog.getMaxOffset() - progressFromOffset;
    }

    private boolean isCommitLogAvailable() {
        return progressFromOffset < commitLog.getMaxOffset();
    }

    private void doFetch() {
        progressFromOffset = defaultLogStore.getStoreCheckpoint().getProgressFromOffset();
        if (progressFromOffset < commitLog.getMinOffset()) {
            progressFromOffset = commitLog.getMinOffset();
        }
        if (progressFromOffset > commitLog.getMaxOffset()) {
            progressFromOffset = commitLog.getMaxOffset();
        }
        for (boolean doNext = true; this.isCommitLogAvailable() && doNext && !isStopped(); ) {

            MappedFileResult result = commitLog.getData(progressFromOffset);
            if (result != null) {
                try {
                    progressFromOffset = result.getStartOffset();

                    for (int readSize = 0; readSize < result.getSize() && doNext && !isStopped(); ) {
                        FetchLogResult fetchLogResult = commitLog.fetchLog(result.getByteBuffer());
                        int size = fetchLogResult.getTotalSize();

                        if (fetchLogResult.isSuccess()) {
                            if (size > 0) {
                                defaultLogStore.doDispatch(fetchLogResult);
                                progressFromOffset += size;
                                readSize += size;
                            } else if (size == 0) {
                                progressFromOffset = commitLog.rollNextFile(progressFromOffset);
                                log.warn("[BUG]read mappedFile end or error. progressFromOffset={},fetchResult={}", progressFromOffset,fetchLogResult);
                                readSize = result.getSize();
                            }
                        } else {

                            if (size > 0) {
                                log.error("[BUG]read total count not equals msg total size. progressFromOffset={}", progressFromOffset);
                                progressFromOffset += size;
                            } else {
                                doNext = false;
                            }
                        }
                    }
                } finally {
                    result.release();
                }
            } else {
                doNext = false;
            }
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        int timeout = 10;
        while (!this.isStopped()) {
            try {
                Thread.sleep(20);
                this.doFetch();
                defaultLogStore.getStoreCheckpoint().setProgressFromOffset(progressFromOffset);
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
                if (timeout-- < 0) {
                    log.error(this.getServiceName() + " service has break by. ", e);
                    break;
                }
            }
        }

        log.info(this.getServiceName() + " service end");
    }

}
