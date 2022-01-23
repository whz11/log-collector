package com.whz.logcollector.store;

/**
 * @author whz
 * @date 2022/1/19 20:16
 **/
public enum AppendMessageStatus {
    OK,
    END_OF_FILE,
    MESSAGE_SIZE_EXCEEDED,
    PROPERTIES_SIZE_EXCEEDED,
    UNKNOWN_ERROR,
}
