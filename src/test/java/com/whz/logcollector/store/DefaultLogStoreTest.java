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

        int count = 100;
        while (count-- > 0) {
            long t1 = System.currentTimeMillis();
            logStore.acceptAsync(buildMessage());
            System.out.println("cost:" + (System.currentTimeMillis() - t1));
            try {
                Thread.sleep(200);
            } catch (Exception e) {

            }
        }
//        System.out.println(System.currentTimeMillis()-t1);

    }

    @AfterEach
    public void destroy() {
        logStore.shutdown();
//        logStore.destroy();

    }

    private LogInner buildMessage() {
        LogInner logInner = new LogInner();
        logInner.setAppName("test");
        String body="23:25:16.925 [main] INFO com.whz.logcollector.store.StoreCheckpoint - store checkpoint file indexMsgTimestamp 0，没错是测试";
        logInner.setBody(body);
        return logInner;
    }
}