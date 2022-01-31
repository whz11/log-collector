package com.whz.logcollector.store.config;

import java.io.File;
import java.time.LocalDate;

/**
 * @author whz
 * @date 2022/1/16 11:21
 **/
public class StorePathConfigHelper {
    public static String getStorePathAppLog(final String rootDir) {
        return rootDir + File.separator + "applog";
    }

    public static String getStorePathCommitLog(final String rootDir) {
        return rootDir + File.separator + "commitlog";
    }

    public static String getAppLogFile(final String rootDir, String app, LocalDate fileDate) {
        return rootDir + File.separator + "applog" + File.separator + app + File.separator + fileDate+".log";
    }

    public static String getStoreCheckpoint(final String rootDir) {
        return rootDir + File.separator + "checkpoint";
    }


    public static String getLockFile(final String rootDir) {
        return rootDir + File.separator + "lock";
    }


}
