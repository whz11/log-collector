package com.whz.logcollector.store;

/**
 * @author whz
 * @date 2022/1/18 15:28
 **/
public enum StoreStatus {
    OK,
    FLUSH_DISK_TIMEOUT,
    SERVICE_NOT_AVAILABLE,
    CREATE_MAPPED_FILE_FAILED,
    MESSAGE_ILLEGAL,
    PROPERTIES_SIZE_EXCEEDED,
    OS_PAGE_CACHE_BUSY,
    UNKNOWN_ERROR,
}