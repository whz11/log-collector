package com.whz.logcollector;

import org.springframework.stereotype.Service;

/**
 * @author whz
 * @date 2022/1/8 16:52
 **/
public interface LogCollectorService {
    void acceptAsync(String appName,String logMessage);
    void mergeFile(String appName);
}
