package com.whz.logcollector.store;

import lombok.extern.slf4j.Slf4j;

/**
 * @author whz
 * @date 2022/1/27 15:53
 **/
@Slf4j
public class CommitCommitLogService extends ServiceThread {
    protected static final int RETRY_TIMES_OVER = 10;

    private long lastCommitTimestamp = 0;

    private final DefaultLogStore defaultLogStore;
    private final MappedFileQueue mappedFileQueue;

    public CommitCommitLogService(DefaultLogStore defaultLogStore, MappedFileQueue mappedFileQueue) {
        this.defaultLogStore = defaultLogStore;
        this.mappedFileQueue = mappedFileQueue;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        // 刷盘线程是否停止
        while (!this.isStopped()) {
            // 提交到FileChannel的时间间隔，只在TransientStorePool 打开的情况下使用，默认0.2s
            int interval = defaultLogStore.getLogStoreConfig().getCommitIntervalCommitLog();
            //每次提交到File至少需要多少个page(默认是4个)
            int commitDataLeastPages = defaultLogStore.getLogStoreConfig().getCommitCommitLogLeastPages();
            // 提交完成间隔时间（默认0.2s）
            int commitDataThoroughInterval =
                    defaultLogStore.getLogStoreConfig().getCommitCommitLogThoroughInterval();
            // 开始时间
            long begin = System.currentTimeMillis();

            if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                this.lastCommitTimestamp = begin;
                commitDataLeastPages = 0;
            }

            try {
                boolean result = mappedFileQueue.commit(commitDataLeastPages);
                long end = System.currentTimeMillis();
                if (!result) {
                    this.lastCommitTimestamp = end; // result = false means some data committed.

                }

                if (end - begin > 500) {
                    log.info("Commit data to file costs {} ms", end - begin);
                }
                // 暂停200ms，再运行
                Thread.sleep(interval);
            } catch (Throwable e) {
                log.error(this.getServiceName() + " service has exception. ", e);
            }
        }

        boolean result = false;
        // 正常关机，循环10次，进行10次的有数据就提交的操作
        for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
            result = mappedFileQueue.commit(0);
            log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
        }
        log.info(this.getServiceName() + " service end");
    }
}
