package com.whz.logcollector.store.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

/**
 * @author whz
 * @date 2022/1/16 20:19
 **/
@Data
public class LogStoreConfig {
    private int commitLogSize = 1024*1024*128;
    private String storePathRootDir = "store";
    private String appLogPathRootDir = "store";
    private int commitIntervalCommitLog = 200;
    private int commitCommitLogLeastPages = 4;
    private int commitCommitLogThoroughInterval = 200;

    //刷盘任务时间间隔，多久刷一次盘?ms
    private int flushIntervalCommitLog = 500;
    // 一次刷写任务至少包含页数，如果待刷写数据不足，小于该参数配置的值，将忽略本次刷写任务,默认4页
    private int flushCommitLogLeastPages = 4;
    // 两次真实刷写任务最大跨度，默认10s
    private int flushCommitLogThoroughInterval = 1000 * 10;

}
