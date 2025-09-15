package dataredis.csdn.configcsdn;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;

/**
 * @Author Nanaan
 * @Date 2025/2/16 0:13
 * @Description TODO
 */

@Configuration
public class ThreadPoolConfig {
    @Bean(name = "threadPoolExecutor")
    public ThreadPoolExecutor threadPoolExecutor() {
        // 配置ThreadPoolExecutor的参数，这里只是示例
        int corePoolSize = 10;
        int maximumPoolSize = 20;
        long keepAliveTime = 100;
        TimeUnit unit = TimeUnit.SECONDS;
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        RejectedExecutionHandler handler = new ThreadPoolExecutor.AbortPolicy();

        ThreadPoolExecutor executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        return executor;
    }
}