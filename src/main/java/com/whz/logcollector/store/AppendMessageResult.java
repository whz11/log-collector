package com.whz.logcollector.store;

import lombok.Data;

/**
 * @author whz
 * @date 2022/1/19 20:14
 **/
@Data
public class AppendMessageResult {
    // Return code
    private AppendMessageStatus status;
    // Where to start writing
    private long wroteOffset;
    // Write Bytes
    private int wroteBytes;

    public AppendMessageResult(AppendMessageStatus status) {
        this(status, 0, 0);
    }

    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
    }
}
