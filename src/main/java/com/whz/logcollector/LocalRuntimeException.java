package com.whz.logcollector;

import java.util.function.Supplier;

/**
 * @author whz
 * @date 2021/4/20 下午2:24
 **/
public class LocalRuntimeException extends RuntimeException implements Supplier<LocalRuntimeException> {
    private String message;


    public LocalRuntimeException(String message) {
        super(message);
        this.message = message;
    }


    @Override
    public LocalRuntimeException get() {
        return this;
    }

    @Override
    public String getMessage(){
        return this.message;
    }
}
