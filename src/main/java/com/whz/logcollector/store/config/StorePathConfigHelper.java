package com.whz.logcollector.store.config;

import java.io.File;

/**
 * @author whz
 * @date 2022/1/16 11:21
 **/
public class StorePathConfigHelper {
    public static String getStorePathConsumeQueue(final String rootDir) {
        return rootDir + File.separator + "consumequeue";
    }

    public static String getStorePathConsumeQueueExt(final String rootDir) {
        return rootDir + File.separator + "consumequeue_ext";
    }

    public static String getStoreCheckpoint(final String rootDir) {
        return rootDir + File.separator + "checkpoint";
    }


    public static String getLockFile(final String rootDir) {
        return rootDir + File.separator + "lock";
    }


}
