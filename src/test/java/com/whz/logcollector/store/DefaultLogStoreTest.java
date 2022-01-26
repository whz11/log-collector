package com.whz.logcollector.store;

import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author whz
 * @date 2022/1/19 22:12
 **/
@RunWith(MockitoJUnitRunner.class)
class DefaultLogStoreTest {
    private LogStore logStore;

    @BeforeEach
    public void init() throws Exception {
        System.out.println("ss");
        logStore = new DefaultLogStore();
        boolean load = logStore.load();
        assertTrue(load);
        logStore.start();
    }
    @Test
    void asyncPut() {
        long t1=System.currentTimeMillis();
        while (true) {
            logStore.asyncPut(buildMessage());
        }
//        System.out.println(System.currentTimeMillis()-t1);

    }
    @AfterEach
    public void destroy() {
//        logStore.shutdown();
//        logStore.destroy();

    }
    private LogInner buildMessage() {
        LogInner logInner=new LogInner();
        logInner.setAppName("test");
        logInner.setBody("大家好我是test".getBytes(StandardCharsets.UTF_8));
        return logInner;
    }
}