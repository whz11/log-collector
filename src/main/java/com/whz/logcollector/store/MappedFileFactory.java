package com.whz.logcollector.store;

import com.whz.logcollector.store.config.StorePathConfigHelper;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ServiceLoader;
import java.util.concurrent.*;

/**
 * @author whz
 * @date 2022/1/19 11:17
 **/
@Slf4j
public class MappedFileFactory extends ServiceThread {

    private static final int WATERLINE = 2;
    private final ConcurrentLinkedQueue<MappedFile> mappedFileWarehouse = new ConcurrentLinkedQueue<>();
    private final BlockingQueue<Boolean> notifyQueue = new ArrayBlockingQueue<>(WATERLINE);
    private final DefaultLogStore logStore;
    private long latestOffset = -1;

    public MappedFileFactory(DefaultLogStore logStore) {
        this.logStore = logStore;
        for (int i = 0; i < WATERLINE; i++) {
            this.notifyQueue.offer(Boolean.TRUE);
        }
    }


    public MappedFile allocate() {
        notifyQueue.offer(Boolean.TRUE);
        log.info("notify MappedFileFactory to create");
        if (mappedFileWarehouse.isEmpty()) {
            log.warn("mappedFileWarehouse isEmpty, wait MappedFileFactory to create");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (mappedFileWarehouse.isEmpty()) {
            log.error("mappedFileWarehouse still Empty");
            return null;
        }
        return mappedFileWarehouse.poll();
    }

    @Override
    public String getServiceName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void shutdown() {
        super.shutdown(true);
        for (MappedFile mappedFile : this.mappedFileWarehouse.toArray(new MappedFile[0])) {
            if (mappedFile != null) {
                log.info("delete pre allocated maped file, {}", mappedFile.getFileName());
                mappedFile.destroy(1000);
            }
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        int fileSize = logStore.getLogStoreConfig().getCommitLogSize();
        while (!this.isStopped()) {
            try {
                //阻塞，等待通知唤醒，减少while死循环占用cpu
                notifyQueue.take();
                long createOffset = latestOffset == -1 ? 0 : latestOffset + fileSize;
                String nextFilePath = StorePathConfigHelper.getStorePathCommitLog(logStore.getLogStoreConfig().getStorePathRootDir()) + File.separator + offset2FileName(createOffset);
                MappedFile curMappedFile = create(nextFilePath, fileSize);
                mappedFileWarehouse.offer(curMappedFile);
                latestOffset = createOffset;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        log.info(this.getServiceName() + " service end");
    }

    public String offset2FileName(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    private MappedFile create(String fileName, int size) {
        MappedFile mappedFile = null;
        try {
            try {
                mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();
                mappedFile.init(fileName, size, logStore.getDirectByteBufferPool());
            } catch (RuntimeException e) {
                log.warn("Use default implementation.");
                mappedFile = new MappedFile(fileName, logStore.getLogStoreConfig().getCommitLogSize(), logStore.getDirectByteBufferPool());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mappedFile;
    }

    public void setLatestOffset(long latestOffset) {
        this.latestOffset = latestOffset;
    }

    public long getLatestOffset() {
        return latestOffset;
    }
}
