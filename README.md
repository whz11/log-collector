# log-collector

日志采集器服务端

存储模块借鉴`rocketmq`，采用文件系统存储，而不采用外部数据库（分析采集器对事务要求不高，对写入速度有要求）。这样使得当前采集器日志吞吐量可以达到单实例3w+/s(8核16g机器)，瓶颈在网络框架是`tomcat`（偷懒用的`springboot`），之后替换成相应的异步框架（`netty`，`akka`，`vert.x`）。

存储模块设计：

 1. 核心写入commitlog文件，为了保证写入性能的稳定（即当接入多个项目后也不会产生影响）采用顺序写的方法，所有日志写入一个文件，数据格式

    | totalsize | app_name_size | app_name | log_size | log  |
    | --------- | ------------- | -------- | -------- | ---- |

    

 2. 多个独立线程服务：FlushCommitLogService（异步刷盘服务，对已提交到filechannel的数据刷盘）、CommitCommitLogService（定时提交服务，将writebuffer中的数据提交到filechannel）、FetchLogService（日志提取服务，将commitlog文件中的新数据提取解析成原始日志文件）、MappedFileFactory（内存映射文件工厂，负责创建mappedfile文件，维持空闲文件数量在waterline，来预防突发流量带来频繁创建mappedfile的抖动）

 3. directByteBufferPool堆外内存池化，把要存储的数据先存入该 buffer 中，然后再需要刷盘的时候，将该 buffer 的数据传入 FileChannel，这样就和 MappedByteBuffer 一样能做到零拷贝了。除此之外，该 Buffer 还使用了 com.sun.jna.Library 类库将该批内存锁定，避免被置换到交换区，提高存储性能。



## Table of Contents

- [QuickStart](https://github.com/whz11/qynat-spring-boot-starter/blob/master/README.md#QuickStart)
- [Maintainers](https://github.com/whz11/qynat-spring-boot-starter/blob/master/README.md#maintainers)
- [License](https://github.com/whz11/qynat-spring-boot-starter/blob/master/README.md#license)



## QuickStart

### 服务端配置

服务端几乎0配置， store的config文件名LogStoreConfig

### 客户端配置

客户端即青柚项目想要接入logcollector，只需在以下几步：

1.在`application.properties`文件中添加

```properties
logging.url=http://xx.xx.xx.xx:8080/logcollector/log/你的项目名称
```

2.在项目代码文件夹下创建class复制以下代码

```java

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.UnsynchronizedAppenderBase;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class LogCollectorAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {

    private Layout<ILoggingEvent> layout;

    private ScheduledExecutorService scheduledExecutor;
    private boolean enabled = true;
    private String servers;
    private long flushPeriod = 5;
    private long threshold = 30;
    private long retry = 3;
    private final int BLOCK_SIZE = 1024 * 8;
    private final AtomicLong committed = new AtomicLong();
    private StringBuffer writeBuffer = new StringBuffer(BLOCK_SIZE);
    private StringBuffer readBuffer = new StringBuffer(BLOCK_SIZE);
    private String currentIp;


    @Override
    public void start() {
        if (isStarted()) {
            return;
        }
        if (layout == null) {
            addError("No layout set for the appender named [" + name + "].");
            return;
        }
        if (servers == null || servers.length() == 0 || "localhost".equals(servers)) {
            addWarn("servers is not valid.");
            enabled = false;
        }

        String[] ips = getCurrentIp().split("\\.");
        if (ips.length != 4) {
            currentIp = ips[0].substring(Math.max(0, currentIp.length() - 6));
        } else {
            currentIp = ips[2] + "." + ips[3];
        }
        ThreadFactory factory = runnable -> {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setName(this.getClass().getName());
            thread.setDaemon(true);
            return thread;
        };
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(factory);
        scheduledExecutor.scheduleWithFixedDelay(this::flush, flushPeriod, flushPeriod, TimeUnit.SECONDS);

        super.start();
        this.addInfo("LogCollectorAppender start");

    }


    @Override
    protected void append(ILoggingEvent event) {
        if (!enabled) {
            return;
        }
        String log = "(" + currentIp + ")" + this.layout.doLayout(event) + "\n";
        writeBuffer.append(log);
        if (committed.incrementAndGet() > threshold || writeBuffer.length() > BLOCK_SIZE * 0.8) {
            flush();
        }
    }

    @Override
    public void stop() {
        super.stop();
        scheduledExecutor.shutdown();
        while (retry-- > 0 && committed.get() > 0) {
            this.addWarn("LogCollectorAppender will stop, try to flush buffer");
            flush();
        }
        this.addInfo("LogCollectorAppender shutdown complete");

    }

    private void flush() {
        if (committed.get() == 0 || !enabled) {
            return;
        }
        try {
            //swap 读写分离
            StringBuffer tempBuffer = writeBuffer;
            writeBuffer = readBuffer;
            readBuffer = tempBuffer;
            send(readBuffer.toString().getBytes(StandardCharsets.UTF_8));
            readBuffer.setLength(0);
            committed.set(0);
        } catch (Exception e) {
            this.addError("scheduled flush exception", e);
        }
    }

    protected HttpURLConnection getHttpConnection(URL url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setRequestProperty("Content-Type", this.layout.getContentType() + "; charset=" + StandardCharsets.UTF_8.name());
        conn.setRequestMethod("POST");
        conn.setReadTimeout(1000);
        return conn;
    }

    private void send(byte[] logBytes) {
        HttpURLConnection conn = null;
        try {
            conn = this.getHttpConnection(new URL(servers));
            BufferedOutputStream out = new BufferedOutputStream(conn.getOutputStream());
            out.write(logBytes);
            out.flush();
            out.close();
            System.out.println("LogCollectorAppender conn code  :" + conn.getResponseCode() + ",servers:" + servers + ",bytes:" + logBytes.length);

        } catch (Exception e) {
            this.addError(" client-side exception", e);
        } finally {
            if (null != conn) {
                try {
                    conn.disconnect();
                } catch (Exception e) {
                    this.addError("conn 流关闭异常：" + e.getLocalizedMessage());
                }
            }
        }
    }

    public String getCurrentIp() {
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface ni = networkInterfaces.nextElement();
                Enumeration<InetAddress> nias = ni.getInetAddresses();
                while (nias.hasMoreElements()) {
                    InetAddress ia = nias.nextElement();
                    if (!ia.isLinkLocalAddress() && !ia.isLoopbackAddress() && ia instanceof Inet4Address) {
                        return ia.getHostAddress();
                    }
                }
            }
        } catch (SocketException e) {

        }
        //获取ip失败采用uuid也可以模拟
        return UUID.randomUUID().toString();
    }

    public Layout<ILoggingEvent> getLayout() {
        return layout;
    }

    public void setLayout(Layout<ILoggingEvent> layout) {
        this.layout = layout;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public String getServers() {
        return servers;
    }

    public void setFlushPeriod(long flushPeriod) {
        this.flushPeriod = flushPeriod;
    }

    public long getFlushPeriod() {
        return flushPeriod;
    }

    public long getThreshold() {
        return threshold;
    }

    public void setThreshold(long threshold) {
        this.threshold = threshold;
    }

    public boolean isEnabled() {
        return enabled;
    }
}
```

3.在项目`logback-spring.xml`中添加刚刚的`appender`（`servers`，即服务端地址，也是我们第一步在配置文件中写的)（`flushPeriod`即发送日志的间隔时间s）(`pattern`是日志格式，此处默认是青柚日志格式)其他参数见上一步代码的字段，在xml中写成标签形式可覆盖。
(`appender的class`部分填写上一步添加的`appender`路径)
```xml
    <springProperty scope="context" name="HTTP_URL" source="logging.url" defaultValue="localhost"/>
 
<!--自定义appender, 起名为http-->
    <appender name="HTTP" class="com.qingyou.annualreport.util.LogCollectorAppender">
        <!--请求的地址-->
        <servers>${HTTP_URL}</servers>
        <flushPeriod>3</flushPeriod>
        <layout class="ch.qos.logback.classic.PatternLayout">
            <pattern>${POMELO_STANDER_FILE_LOG_PATTERN}</pattern>
        </layout>
        <filter class="ch.qos.logback.classic.filter.ThresholdFilter">
            <level>INFO</level>
        </filter>
    </appender>
```

4.最后一步，在生产环境中引入自定义的`appender`

```xml
  <!--生产环境：生成日志-->
    <springProfile name="prod">
        <root level="INFO">
            <appender-ref ref="HTTP"/>
        </root>
    </springProfile>
```



## Maintainers

[@whz11](https://github.com/whz11/).



## Contributing

Feel free to dive in! Open an issue or submit PRs.



## License

[GPL](https://git.qingyou.ren/Pomelo/backend/log-collector/-/blob/master/LICENSE) © whz11

<details class="details-reset details-overlay details-overlay-dark" id="jumpto-line-details-dialog" style="box-sizing: border-box; display: block;"><summary data-hotkey="l" aria-label="Jump to line" role="button" style="box-sizing: border-box; display: list-item; cursor: pointer; list-style: none;"></summary></details>
