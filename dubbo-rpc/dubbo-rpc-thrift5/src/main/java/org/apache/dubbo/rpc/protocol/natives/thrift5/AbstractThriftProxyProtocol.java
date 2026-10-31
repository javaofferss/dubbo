
package org.apache.dubbo.rpc.protocol.natives.thrift5;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.InvokeMode;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.protocol.AbstractInvoker;
import org.apache.dubbo.rpc.protocol.AbstractProxyProtocol;
import org.apache.dubbo.rpc.protocol.natives.thrift5.support.Thrift5AttachmentHolder;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.dubbo.rpc.protocol.natives.thrift5.ThreadlessDirectExecutor.DIRECT_EXECUTOR;

/*
 * create by cmj.
 * 因为thrift0.5走的是BIO. 要做同步转异步处理。这样才能支持dubbo的所有能力。
 * 因为dubbo的协议是基于非阻塞NIO实现的。有一些超时机制功能。如果用BIO这些超时机制将不能生效.
 * 所以要转成异步调用.
 */
public abstract class AbstractThriftProxyProtocol extends AbstractProxyProtocol {

    public AbstractThriftProxyProtocol(Class<?>... exceptions) {
        super(exceptions);
    }

    @Override
    protected <T> Invoker<T> protocolBindingRefer(final Class<T> type, final URL url) throws RpcException {
        final Invoker<T> target = proxyFactory.getInvoker(doRefer(type, url), type, url);
        ExecutorService executorServiceInvoker = getOrCreateCallbackExecutor(url);
        Invoker<T> invoker = new AbstractInvoker<T>(type, url) {
            ExecutorService executorService = executorServiceInvoker;
            @Override
            protected Result doInvoke(Invocation invocation) throws Throwable {
                try {
                    // fix @Method Take effect
                    int timeout = RpcUtils.calculateTimeout(getUrl(), invocation, RpcUtils.getMethodName(invocation), 3000);
                    if (timeout <= 0) {
                        return AsyncRpcResult.newDefaultAsyncResult(
                                new RpcException(
                                        RpcException.TIMEOUT_TERMINATE,
                                        "No time left for making the following call: " + invocation.getServiceName() + "."
                                                + RpcUtils.getMethodName(invocation) + ", terminate directly."),
                                invocation);
                    }

                    //参考DubboInvoker. 这里直接做timeout检查，不依赖 waitForResultIfSync.
                    ExecutorService executor = getCallbackExecutor(getUrl(), invocation, timeout);

                    Map<String, Object> hdr = new LinkedHashMap<>();
                    //可以看这个类 {@link ConsumerContextFilter} 支持 attachments 全链路透传.
                    Map<String, Object> invAtt = invocation.getObjectAttachments();
                    if (invAtt != null) {
                        hdr.putAll(invAtt); // tracing/token 等外层 filter 注入的在此
                    }
                    Map<String, Object> clientAtt = RpcContext.getClientAttachment().getObjectAttachments();
                    if (clientAtt != null && !clientAtt.isEmpty()) {
                        hdr.putAll(clientAtt); // 用户直接 RpcContext.setAttachment 的在此
                    }

                    Result result = executor.submit(() -> {
                        try {
                            Thrift5AttachmentHolder.set(hdr);
                            return target.invoke(invocation);
                        } finally {
                            Thrift5AttachmentHolder.clear();
                        }
                    }).get(timeout, TimeUnit.MILLISECONDS);
                    // FIXME result is an AsyncRpcResult instance.
                    Throwable e = result.getException();

                    if (e != null) {
                        for (Class<?> rpcException : rpcExceptions) {
                            if (rpcException.isAssignableFrom(e.getClass())) {
                                throw getRpcException(type, url, invocation, e);
                            }
                        }
                    }
                    return result;
                } catch (TimeoutException  e) {
                    throw new RpcException(
                            RpcException.TIMEOUT_EXCEPTION,
                            "Invoke remote method timeout. method: " + RpcUtils.getMethodName(invocation) + ", provider: "
                                    + getUrl() + ", cause: " + e.getMessage(),
                            e);
                } catch (RpcException e) {
                    if (e.getCode() == RpcException.UNKNOWN_EXCEPTION) {
                        e.setCode(getErrorCode(e.getCause()));
                    }
                    throw e;
                } catch (Throwable e) {
                    throw getRpcException(type, url, invocation, e);
                }
            }

            protected ExecutorService getCallbackExecutor(URL url, Invocation inv, int timeout) {
                if (timeout == Integer.MAX_VALUE && InvokeMode.SYNC == RpcUtils.getInvokeMode(getUrl(), inv)) {
                    return DIRECT_EXECUTOR;
                }
                return executorService;
            }

            @Override
            public void destroy() {
                super.destroy();
                target.destroy();
                invokers.remove(this);
                AbstractThriftProxyProtocol.this.destroyInternal(url);
            }
        };
        invokers.add(invoker);
        return invoker;
    }

    protected ExecutorService getOrCreateCallbackExecutor(URL url) {
        return ExecutorRepository.getInstance(url.getOrDefaultApplicationModel())
                .createExecutorIfAbsent(url);
    }

}
