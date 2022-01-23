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
    private int commitLogSize = 1024 * 1024 * 512;
    //    private int mappedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQ_STORE_UNIT_SIZE;
    private String storePathRootDir = "store";

    private String storePathCommitLog = "store"
            + File.separator + "commitlog";
}
