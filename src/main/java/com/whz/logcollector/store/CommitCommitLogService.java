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
            // 触发commit机制有两种方式：1.commit时间超过了两次commit时间间隔，然后只要有数据就进行提交 2.commit数据页数大于默认设置的4页
            // 本次commit时间>上次commit时间+两次commit时间间隔，则进行commit，不用关心commit页数的大小，设置commitDataLeastPages=0
            if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                this.lastCommitTimestamp = begin;
                commitDataLeastPages = 0;
            }

            try {
                // result=false，表示提交了数据，多与上次提交的位置；表示此次有数据提交；result=true，表示没有新的数据被提交

//                    MappedFile的刷盘方式有两种：
//                    1. 写入内存字节缓冲区(writeBuffer) ----> 从内存字节缓冲区(write buffer)提交(commit)到文件通道(fileChannel) ----> 文件通道(fileChannel)flush到磁盘
//                    2.写入映射文件字节缓冲区(mappedByteBuffer) ----> 映射文件字节缓冲区(mappedByteBuffer)flush
                //执行提交的核心逻辑：将byteBuffer数据提交到FileChannel
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
