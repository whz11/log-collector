package com.whz.logcollector.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.whz.logcollector.store.config.LogStoreConfig;
import com.whz.logcollector.store.util.CUtil;
import lombok.extern.slf4j.Slf4j;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * @author whz
 * @date 2022/1/16 20:18
 **/
@Slf4j
public class DirectByteBufferPool {
    private final int poolSize;
    private final int fileSize;
    private final Deque<ByteBuffer> availableBuffers;
    public DirectByteBufferPool() {
        this.poolSize = 2;
        this.fileSize = 1024*1024*512;
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }
    public DirectByteBufferPool(final LogStoreConfig storeConfig) {
        this.poolSize = 5;
        this.fileSize = storeConfig.getCommitLogSize();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }


    public void init() {
        for (int i = 0; i < poolSize; i++) {
            //不使用JVM堆栈而是通过操作系统来创建内存块用作缓冲区，它与当前操作系统能够更好的耦合，
            // 因此能进一步提高I/O操作速度。但是分配直接缓冲区的系统开销很大，因此只有在缓冲区较大并长期存在，
            // 或者需要经常重用时，才使用这种缓冲区
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            //将当前堆外内存一直锁定在内存中，避免被进程将内存交换到磁盘。
            CUtil.INSTANCE.mlock(pointer, new NativeLong(fileSize));
            availableBuffers.offer(byteBuffer);
        }
    }

    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            //解除锁定的内存
            CUtil.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("DirectByteBufferPool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    public int availableBufferNums() {
        return availableBuffers.size();
    }
}
