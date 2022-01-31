package com.whz.logcollector.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author whz
 * @date 2022/1/18 23:06
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogInner {
    private String appName;
    private long commitLogOffset;
    private String body;
    public LogInner (String appName,String body){
        this.appName=appName;
        this.body=body;
    }
}
