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
    private volatile long progressFromOffset = 0;
    private final DefaultLogStore defaultLogStore;
    private final CommitLog commitLog;

    public FetchLogService(DefaultLogStore defaultLogStore, CommitLog commitLog) {
        this.defaultLogStore = defaultLogStore;
        this.commitLog = commitLog;
    }

    @Override
    public void shutdown() {
        for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
        }

        if (this.isCommitLogAvailable()) {
            log.warn("shutdown LogCollectService, but commitlog have not finish to be dispatched, commitOffset: {} progressFromOffset: {}",
                    commitLog.getMaxOffset(), progressFromOffset);
        }

        super.shutdown();
    }

    public long behind() {
        return commitLog.getMaxOffset() - progressFromOffset;
    }

    private boolean isCommitLogAvailable() {
        return this.progressFromOffset < commitLog.getMaxOffset();
    }

    private void doFetch() {
        progressFromOffset = defaultLogStore.getStoreCheckpoint().getProgressFromOffset();
        if (progressFromOffset < commitLog.getMinOffset()) {
            progressFromOffset = commitLog.getMinOffset();
        }
        if (progressFromOffset > commitLog.getMaxOffset()) {
            progressFromOffset = commitLog.getMaxOffset();
        }
        for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {

            MappedFileResult result = commitLog.getData(progressFromOffset);
            if (result != null) {
                try {
                    progressFromOffset = result.getStartOffset();

                    for (int readSize = 0; readSize < result.getSize() && doNext; ) {
                        FetchLogResult fetchLogResult = commitLog.fetchLog(result.getByteBuffer());
                        int size = fetchLogResult.getTotalSize();

                        if (fetchLogResult.isSuccess()) {
                            if (size > 0) {
                                defaultLogStore.doDispatch(fetchLogResult);
                                progressFromOffset += size;
                                readSize += size;
                            } else if (size == 0) {
                                progressFromOffset = commitLog.rollNextFile(progressFromOffset);
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

        while (!this.isStopped()) {
            try {
                Thread.sleep(20);
                this.doFetch();
                defaultLogStore.getStoreCheckpoint().setProgressFromOffset(progressFromOffset);
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

}
