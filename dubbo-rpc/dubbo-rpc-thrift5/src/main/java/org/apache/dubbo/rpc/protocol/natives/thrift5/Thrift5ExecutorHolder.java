package org.apache.dubbo.rpc.protocol.natives.thrift5;

import java.util.concurrent.Callable;

public class Thrift5ExecutorHolder {

    private static final ThreadLocal<ThriftExecutor> EXECUTORS = new ThreadLocal<>();

    public static void set(ThriftExecutor executorInfo) {
        EXECUTORS.set(executorInfo);
    }

    public static ThriftExecutor get() {
        return EXECUTORS.get();
    }

    /** 工作线程复用，每个请求结束必须 clear，否则脏附件泄漏到下一请求。 */
    public static void clear() {
        EXECUTORS.remove();
    }

    public static Object exec(Callable<?> task) throws Exception {
        return get().execReturn(task);
    }

    public static void exec(Runnable task) throws Exception {
         get().execVoid(task);
    }

    public static void main(String[] args) throws Exception {
        Thrift5ExecutorHolder.exec(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return null;
            }
        });

        Thrift5ExecutorHolder.exec(new Runnable() {

            @Override
            public void run() {

            }
        });
    }
}
