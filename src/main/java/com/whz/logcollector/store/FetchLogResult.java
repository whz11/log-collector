package com.whz.logcollector.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author whz
 * @date 2022/1/27 21:28
 **/
@Data
@AllArgsConstructor
public class FetchLogResult {
    private final String app;
    private String body;
    private int  bodySize;
    private int totalSize;
    private final boolean success;

    public FetchLogResult(int size, boolean success) {
        this.app = "";
        this.totalSize = size;
        this.success = success;
    }
}
