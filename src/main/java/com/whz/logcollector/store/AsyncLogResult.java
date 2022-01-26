package com.whz.logcollector.store;

import lombok.Data;

/**
 * @author whz
 * @date 2022/1/19 20:14
 **/
@Data
public class AsyncLogResult {
    private AsyncLogStatus status;
    private long wroteOffset;
    private int wroteBytes;

    public AsyncLogResult(AsyncLogStatus status) {
        this(status, 0, 0);
    }

    public AsyncLogResult(AsyncLogStatus status, long wroteOffset, int wroteBytes) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
    }
}
