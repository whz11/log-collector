package com.whz.logcollector.store;

/**
 * @author whz
 * @date 2022/1/19 20:16
 **/
public enum AsyncLogStatus {
    OK,
    END_OF_FILE,
    LOG_SIZE_EXCEEDED,
    PROPERTIES_SIZE_EXCEEDED,
    UNKNOWN_ERROR,
}
