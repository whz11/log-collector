package com.whz.logcollector.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.whz.logcollector.store.util.CUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import sun.nio.ch.DirectBuffer;
import sun.nio.ch.FileChannelImpl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author whz
 * @date 2022/1/16 16:37
 **/
@EqualsAndHashCode(callSuper = true)
@Slf4j
@Data
public class MappedFile extends ReferenceResource {
    public static final int OS_PAGE_SIZE = 1024 * 4;
    public static int MAX_LOG_SIZE = 1024 * 1024 * 4;

    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    //当前文件所映射到的消息写入page cache的位置
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    protected final AtomicInteger flushedPosition = new AtomicInteger(0);
    protected int fileSize;
    protected FileChannel fileChannel;
    protected ByteBuffer writeBuffer = null;
    protected DirectByteBufferPool directByteBufferPool = null;
    private String fileName;
    private long fileFromOffset;
    private File file;
    private MappedByteBuffer mappedByteBuffer;
    private volatile long storeTimestamp = 0;
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize,
                      final DirectByteBufferPool directByteBufferPool) throws IOException {
        init(fileName, fileSize, directByteBufferPool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }


    public void init(final String fileName, final int fileSize,
                     final DirectByteBufferPool directByteBufferPool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = directByteBufferPool.borrowBuffer();
        this.directByteBufferPool = directByteBufferPool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public AsyncLogResult appendLogInner(final LogInner logInner) {

        int currentPos = this.wrotePosition.get();

        if (currentPos < this.fileSize) {
            ByteBuffer byteBuffer = writeBuffer.slice();
            byteBuffer.position(currentPos);

            AsyncLogResult result = this.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, logInner);
            this.wrotePosition.addAndGet(result.getWroteBytes());
            return result;
        }
        return new AsyncLogResult(AsyncLogStatus.UNKNOWN_ERROR);
    }

    private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
        byteBuffer.flip();
        byteBuffer.limit(limit);
    }

    public AsyncLogResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                   final LogInner logInner) {
        long wroteOffset = fileFromOffset + byteBuffer.position();

        byte[] appNameBytes = new byte[0];
        byte[] bodyBytes = new byte[0];

        final int bodyLength = logInner.getBody() == null ? 0 : (bodyBytes = logInner.getBody().getBytes(StandardCharsets.UTF_8)).length;
        final int appNameLength = logInner.getAppName() == null ? 0 : (appNameBytes = logInner.getAppName().getBytes(StandardCharsets.UTF_8)).length;
        final int logBlockLen = 4 //log total size
                + 4 + appNameLength //appNameLength and appName
                + 4 + bodyLength //bodyLength and body
                ;

        //log单条最大4m
        if (logBlockLen > MAX_LOG_SIZE) {
            log.warn("block size exceeded, block total size: " + logBlockLen + ", block body size: " + bodyLength
                    + ", MAX_LOG_SIZE: " + MAX_LOG_SIZE);
            return new AsyncLogResult(AsyncLogStatus.LOG_SIZE_EXCEEDED);
        }

        // 没有充足空间
        if (logBlockLen > maxBlank) {
            return new AsyncLogResult(AsyncLogStatus.END_OF_FILE, wroteOffset, maxBlank);
        }
        ByteBuffer logBlock = ByteBuffer.allocate(logBlockLen);

        // 1 log total size
        logBlock.putInt(logBlockLen);

        // 2 appNameLength
        logBlock.putInt(appNameLength);

        // 3 appName
        if (appNameLength > 0) {
            logBlock.put(appNameBytes);
        }

        // 4 bodyLength
        logBlock.putInt(bodyLength);

        // 5 body
        if (bodyLength > 0) {
            logBlock.put(bodyBytes);
        }

        byteBuffer.put(logBlock.array(), 0, logBlockLen);


        return new AsyncLogResult(AsyncLogStatus.OK, wroteOffset, logBlockLen);
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }


    /**
     * 将内存数据刷写到磁盘
     * 数据会在fileChannel或者MappedByteBuffer中。在MappedFile的设计中，只有提交了的数据，
     * 写入到了MappedByteBuffer或者FileChannel中的数据认为是安全的数据
     * 实际上只是将数据写入 PageCache，而操作系统会自动的将脏页刷盘，这层 PageCache 就是我们应用和物理存储之间的夹层，
     * 当我们将数据写入 PageCache 后，即便我们的应用崩溃了，但是只要系统不崩溃，最终也会将数据刷入磁盘。
     * 所以，以写入 PageCache 作为数据安全可读的判断标准。
     */
    public int flush(final int flushLeastPages) {
        long start = System.currentTimeMillis();
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();
                try {
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }
                long gap = System.currentTimeMillis() - start;
                if (gap > 500) {
                    log.warn("mappedfile flush 耗时:{}ms,flushedDiff:{}B", gap, this.flushedPosition.get() - value);
                }
                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition().get();
    }

    //将writeBuffer数据写入FileChannel
    public int commit(final int commitLeastPages) {

        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0();
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.directByteBufferPool != null && this.fileSize == this.committedPosition.get()) {
            //归还到堆外内存池中，并且释放当前writeBuffer
            this.directByteBufferPool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    protected void commit0() {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - lastCommittedPosition > 0) {
            try {
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }


    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public MappedFileResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new MappedFileResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    public MappedFileResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new MappedFileResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
//        if (this.isAvailable()) {
//            log.error("this file[REF:" + currentRef + "] " + this.fileName
//                    + " have not shutdown, stop unmapping.");
//            return false;
//        }
//
//        if (this.isCleanupOver()) {
//            log.error("this file[REF:" + currentRef + "] " + this.fileName
//                    + " have cleanup, do not do it again.");
//            return true;
//        }
//
//        clean(this.mappedByteBuffer);
//        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
//        TOTAL_MAPPED_FILES.decrementAndGet();
//        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * MappedByteBuffer 的释放过程实际上有些诡异，Java 官方没有提供公共的方法来进行 MappedByteBuffer 的回收，
     * 所以不得不通过反射来进行回收，这也是 MappedByteBuffer 比较坑的一点。
     */
    public static void clean(final MappedByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0) {
            return;
        }
        try {
            Method m = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
            m.setAccessible(true);
            m.invoke(FileChannelImpl.class, buffer);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                        + this.getFlushedPosition() + ", "
                        + (System.currentTimeMillis() - beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public AtomicInteger getCommittedPosition() {
        return committedPosition;
    }

    public AtomicInteger getFlushedPosition() {
        return flushedPosition;
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public void warmMappedFile(int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);

        this.mlock();
    }


    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }


    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = CUtil.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = CUtil.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), CUtil.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = CUtil.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
