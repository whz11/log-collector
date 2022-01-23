package com.whz.logcollector.store;

import java.util.concurrent.CompletableFuture;

/**
 * @author whz
 * @date 2022/1/17 16:15
 **/
public interface LogStore {
    /**
     * Load previously stored messages.
     *
     * @return true if success; false otherwise.
     */
    boolean load();

    /**
     * Launch this message store.
     *
     * @throws Exception if there is any error.
     */
    void start() throws Exception;

    /**
     * Shutdown this message store.
     */
    void shutdown();

    /**
     * Destroy this message store. Generally, all persistent files should be removed after invocation.
     */
    void destroy();
    CompletableFuture<Boolean> asyncPut(LogInner logInner);
}
