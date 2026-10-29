package org.apache.dubbo.rpc.protocol.natives.thrift5;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 无线线程池，直接执行.
 */
public class ThreadlessDirectExecutor extends AbstractExecutorService {

    final static ThreadlessDirectExecutor DIRECT_EXECUTOR = new ThreadlessDirectExecutor();

    @Override
    public void shutdown() {
        //ignore
    }

    @Override
    public List<Runnable> shutdownNow() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public boolean isShutdown() {
        return true;
    }

    @Override
    public boolean isTerminated() {
        return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return true;
    }

    @Override
    public void execute(Runnable command) {
        command.run();
    }
}
