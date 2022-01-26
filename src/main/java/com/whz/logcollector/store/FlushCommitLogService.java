//package com.whz.logcollector.store;
//
//import lombok.extern.slf4j.Slf4j;
//
///**
// * @author whz
// * @date 2022/1/25 16:33
// **/
//@Slf4j
//public class FlushCommitLogService extends ServiceThread {
//    protected static final int RETRY_TIMES_OVER = 10;
//    private long lastFlushTimestamp = 0;
//    private long printTimes = 0;
//    private final DefaultLogStore defaultLogStore;
//    private final MappedFileQueue mappedFileQueue;
//
//    public FlushCommitLogService(DefaultLogStore defaultLogStore,MappedFileQueue mappedFileQueue){
//        this.defaultLogStore=defaultLogStore;
//        this.mappedFileQueue=mappedFileQueue;
//    }
//    @Override
//    public void run() {
//        log.info(this.getServiceName() + " service started");
//
//        while (!this.isStopped()) {
//            // 默认值为false，表示await方法等待，如果为true，表示使用Thread.sleep方法等待
//            boolean flushCommitLogTimed = defaultLogStore.getLogStoreConfig().isFlushCommitLogTimed();
//            // 刷盘任务时间间隔，多久刷一次盘500ms
//            int interval = defaultLogStore.getLogStoreConfig().getFlushIntervalCommitLog();
//            // 一次刷写任务至少包含页数，如果待刷写数据不足，小于该参数配置的值，将忽略本次刷写任务,默认4页
//            int flushPhysicQueueLeastPages = defaultLogStore.getLogStoreConfig().getFlushCommitLogLeastPages();
//            // 两次真实刷写任务最大跨度，默认10s
//            int flushPhysicQueueThoroughInterval =
//                    defaultLogStore.getLogStoreConfig().getFlushCommitLogThoroughInterval();
//
//            // 打印记录日志标志
//            boolean printFlushProgress = false;
//
//            // Print flush progress
//            long currentTimeMillis = System.currentTimeMillis();
//            // 触发刷盘机制有两种方式：1.刷盘时间超过了两次刷盘时间间隔，然后只要有数据就进行提交 2.commit数据页数大于默认设置的4页
//            // 本次刷盘时间>上次刷盘时间+两次刷盘时间间隔，则进行刷盘，不用关心刷盘页数的大小，设置commitDataLeastPages=0
//            if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
//                this.lastFlushTimestamp = currentTimeMillis;
//                flushPhysicQueueLeastPages = 0;
//                // 每间隔10次记录一次刷盘日志
//                printFlushProgress = (printTimes++ % 10) == 0;
//            }
//
//            try {
//                // 刷盘之前，进行线程sleep/countdownlatch等待
//                if (flushCommitLogTimed) {
//                    Thread.sleep(interval);
//                } else {
//                    this.waitForRunning(interval);
//                }
//
//                if (printFlushProgress) {
//                    this.printFlushProgress();
//                }
//
//                long begin = System.currentTimeMillis();
//                // 刷盘
//                mappedFileQueue.flush(flushPhysicQueueLeastPages);
//                long storeTimestamp = mappedFileQueue.getStoreTimestamp();
//                // 更新checkpoint最后刷盘时间
//                if (storeTimestamp > 0) {
//                    defaultLogStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
//                }
//                long past = System.currentTimeMillis() - begin;
//                if (past > 500) {
//                    log.info("Flush data to disk costs {} ms", past);
//                }
//            } catch (Throwable e) {
//                log.warn(this.getServiceName() + " service has exception. ", e);
//                this.printFlushProgress();
//            }
//        }
//        // while循环结束，正常关机，保证所有的数据刷写到磁盘
//        // Normal shutdown, to ensure that all the flush before exit
//        boolean result = false;
//        for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
//            result = mappedFileQueue.flush(0);
//           log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
//        }
//
//        log.info(this.getServiceName() + " service end");
//    }
//
//
//    @Override
//    public long getJointime() {
//        return 1000 * 60 * 5;
//    }
//}
