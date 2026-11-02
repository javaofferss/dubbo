package org.apache.dubbo.rpc.protocol.natives.thrift5;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ThriftExecutor {

    private ExecutorService executor;

    private int timeOut;

    public ThriftExecutor(ExecutorService executor, int timeOut) {
        this.executor = executor;
        this.timeOut = timeOut;
    }

    public Object execReturn(Callable<?> task) throws Exception {
        return executor.submit(task).get(timeOut, TimeUnit.MILLISECONDS);
    }

    public void execVoid(Runnable task) throws Exception {
         executor.submit(task).get(timeOut, TimeUnit.MILLISECONDS);
    }
}
