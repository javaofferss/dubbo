/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.protocol.natives.thrift5;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.protocol.AbstractProxyProtocol;
import org.apache.dubbo.rpc.protocol.natives.thrift5.transport.DubboHeaderClientTransport;
import org.apache.dubbo.rpc.protocol.natives.thrift5.transport.DubboHeaderInputProtocolFactory;
import org.apache.dubbo.rpc.protocol.natives.thrift5.transport.DubboHeaderOutputProtocolFactory;

import java.lang.reflect.Constructor;
import java.net.SocketException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.*;

/**
 * 支持thrift序列化0.5版本
 */
public class Thrift5Protocol extends AbstractThriftProxyProtocol {

    //记录提供了多少个服务和使用了多少个服务.
    public static final Set<Class> Export = new ConcurrentHashSet<>();
    public static final Set<Class> Refer = new ConcurrentHashSet<>();

    public static final int DEFAULT_PORT = 40880;
    public static final String NAME = "thrift5";
    public static final String THRIFT_ASYNC_IFACE_OPEN = "AsyncIface";
    public static final String THRIFT_IFACE = "$Iface";
    // 注意早期的0.5版本AsyncIface只支持客户端使用。服务端不支持实现.
    public static final String THRIFT_ASYNC_IFACE = "$AsyncIface";
    public static final String THRIFT_PROCESSOR = "$Processor";
    public static final String THRIFT_CLIENT = "$Client";
    // AsyncClient
    public static final String THRIFT_ASYNC_CLIENT = "$AsyncClient";

    Map<String, ThriftServer> servers = new ConcurrentHashMap<>();

    public Thrift5Protocol() {
        super(SocketException.class);
    }

    @Override
    protected <T> Runnable doExport(T impl, Class<T> type, URL url) throws RpcException {
        String key = url.getAddress() + "_" + url.getPort();
        if (servers.containsKey(key)) {
            ThriftServer thriftServer = servers.get(key);
            if (!thriftServer.server.isStopped()) {
                return thriftServer.runnable;
            }
            servers.remove(key);
        }

        servers.computeIfAbsent(key, k -> {
            ThriftServer server = processExport(url.getPort(), getThriftProcessor(impl, type), url);
            return server;
        });
        Export.add(type);
        return servers.get(key).runnable;
    }

    @Override
    protected <T> T doRefer(Class<T> type, URL url) throws RpcException {
        //        T t = processRefer(type, url);
        BiFunction<Class<T>, URL, T> processRefer = this::processRefer;
        Object proxyObject = null;
        try {
            // 返回的是资源池. 和 url是一对一的
            proxyObject = ThriftPoolDirectProxy.getProxyObject(type, url, (BiFunction) processRefer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Refer.add(type);
        return (T) proxyObject;
    }

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    public <T> TProcessor getThriftProcessor(T impl, Class<T> type) {
        String typeName = type.getName();
        if (!typeName.endsWith(THRIFT_IFACE) && !typeName.endsWith(THRIFT_ASYNC_IFACE)) {
            throw new RpcException(String.format("%s is not a thrift interface", typeName));
        }
        int idx = Math.max(typeName.indexOf(THRIFT_IFACE), typeName.indexOf(THRIFT_ASYNC_IFACE));
        String classNameProcessor = typeName.substring(0, idx) + THRIFT_PROCESSOR;
        try {
            // com.javaoffers.dubbo.producer.TOrderService.Processor
            Class<?> clazz = Class.forName(classNameProcessor);
            Constructor constructor = clazz.getConstructor(type);
            return (TProcessor) constructor.newInstance(impl);
        } catch (Exception e) {
            throw new RpcException(String.format("%s is not a thrift processor", classNameProcessor));
        }
    }

    //参考旧的rpc项目设置 ：com.xiaomi.miui.ad.infra.thrift.ThriftServiceRunner#startThriftServerImpl
    //线程池参数全部支持通过 URL 配置覆盖，未配置时沿用历史默认值（和旧逻辑保持一致）：
    // thrift.server.coreThreads / thrift.server.maxThreads / thrift.server.keepAliveSeconds
    // / thrift.server.queueCapacity / thrift.server.threadName / thrift.server.threadGroup
    // / thrift.server.daemon / thrift.server.rejectedExecutionPolicy
    public THsHaServer createServer(
            TNonblockingServerTransport serverTransport, TProcessor processor, TProtocolFactory protocolFactory,
            URL url) {
        ExecutorService executorService = buildExecutorService(url);

        // fork 版 0.5.0-mdf：无 Args builder，inputProtocolFactory / outputProtocolFactory / executorService
        // 全部经构造器参数传入。transportFactory 默认 TFramedTransport.Factory，显式传入保持不变。
        return new THsHaServer(
                new TProcessorFactory(processor),
                serverTransport,
                new TFramedTransport.Factory(),
                // 请求方向：eager 消费 Dubbo 头 → holder
                new DubboHeaderInputProtocolFactory(protocolFactory),
                // 响应方向：writeMessageBegin 先写 Dubbo 响应头（阶段一空头）
                new DubboHeaderOutputProtocolFactory(),
                executorService,
                new THsHaServer.Options());
    }

    // 构建 THsHaServer 的工作线程池。所有参数均可由 URL 覆盖，默认值与历史实现一致。
    private ExecutorService buildExecutorService(URL url) {
        int defaultThreads = Runtime.getRuntime().availableProcessors() * 2;
        return new ThreadPoolExecutor(
                url.getParameter("thrift.server.coreThreads", defaultThreads),
                url.getParameter("thrift.server.maxThreads", defaultThreads),
                url.getParameter("thrift.server.keepAliveSeconds", 60L),
                TimeUnit.SECONDS,
                // 默认无边界队列，可能内存溢出，与旧逻辑保持一致
                new LinkedBlockingQueue<>(url.getParameter("thrift.server.queueCapacity", Integer.MAX_VALUE)),
                new ThriftWorkerThreadFactory(
                        url.getParameter("thrift.server.threadGroup", "Thrift-Workers"),
                        url.getParameter("thrift.server.threadName", "worker-"),
                        url.getParameter("thrift.server.daemon", false)),
                buildRejectedExecutionHandler(
                        url.getParameter("thrift.server.rejectedExecutionPolicy", "CallerRunsPolicy")));
    }

    // 拒绝策略按名称映射，未识别或为空时退回默认 CallerRunsPolicy（和旧逻辑保持一致）
    private static RejectedExecutionHandler buildRejectedExecutionHandler(String policy) {
        if (policy == null) {
            return new ThreadPoolExecutor.CallerRunsPolicy();
        }
        switch (policy) {
            case "AbortPolicy":
                return new ThreadPoolExecutor.AbortPolicy();
            case "DiscardPolicy":
                return new ThreadPoolExecutor.DiscardPolicy();
            case "DiscardOldestPolicy":
                return new ThreadPoolExecutor.DiscardOldestPolicy();
            case "CallerRunsPolicy":
            default:
                return new ThreadPoolExecutor.CallerRunsPolicy();
        }
    }

    // THsHaServer 工作线程工厂：固定线程组 + 自增编号命名，默认非守护线程确保任务完成
    private static final class ThriftWorkerThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final String namePrefix;
        private final boolean daemon;
        private final AtomicInteger counter = new AtomicInteger();

        ThriftWorkerThreadFactory(String groupName, String namePrefix, boolean daemon) {
            this.group = new ThreadGroup(groupName);
            this.namePrefix = namePrefix;
            this.daemon = daemon;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + counter.getAndIncrement());
            t.setDaemon(daemon);
            return t;
        }
    }

    public ThriftServer processExport(int port, TProcessor processor, URL url) {
        try {
            ThriftServer thriftServer = new ThriftServer();
            // 1. 创建处理器
            //            TOrderService.Processor processor =
            //                    new TOrderService.Processor(new TOrderServiceImpl());

            // 2. 创建非阻塞ServerSocket，监听端口9090
            TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);

            // 3. 配置协议工厂，注意服务端和客户端的协议要保持一致
            TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();

            // 4. 创建非阻塞服务器（fork 版 0.5.0-mdf 无 Args builder，直接构造）
            THsHaServer server = createServer(serverTransport, processor, protocolFactory, url);

            // 6. 启动服务（会阻塞主线程）
            new Thread(() -> {
                        server.serve();
                    })
                    .start();

            System.out.println("\nThrift server started at port " + port);
            thriftServer.server = server;
            thriftServer.runnable = () -> {
                if (!server.isStopped()) {
                    server.stop();
                }
            };
            return thriftServer;
        } catch (TTransportException e) {
            e.printStackTrace();
            throw new RpcException(e.getMessage());
        }
    }

    private <T> T processRefer(Class<T> type, URL url) throws RpcException {

        try {
            T thriftClient = null;
            String typeName = type.getName();
//            String path = url.getPath();

            int idx = Math.max(typeName.indexOf(THRIFT_ASYNC_IFACE), typeName.indexOf(THRIFT_IFACE));
            String asyncIfaceClassName = typeName.substring(0, idx) + THRIFT_ASYNC_IFACE;
            Class<?> asyncIfaceClass = Class.forName(asyncIfaceClassName);
            String syncIfaceClassName = typeName.substring(0, idx) + THRIFT_IFACE;
            Class<?> syncIfaceClass = Class.forName(syncIfaceClassName);
            if (typeName.endsWith(THRIFT_IFACE)) {
                String clientClsName = typeName.substring(0, typeName.indexOf(THRIFT_IFACE)) + THRIFT_CLIENT;
                Class<?> clazz = Class.forName(clientClsName);
                Constructor constructor = clazz.getConstructor(TProtocol.class);
                try {
                    TSocket tSocket = new TSocket(url.getHost(), url.getPort());
                    // 这里还可以对tSocket设置项
                    tSocket.setTimeout(1000 * 60); // 60秒超时
                    TTransport framed = new TFramedTransport(tSocket);
                    // DubboHeaderClientTransport：flush 时从 holder 取附件写 Dubbo 请求头、读响应时剥头回填 RpcContext
                    DubboHeaderClientTransport header = new DubboHeaderClientTransport(framed);
                    TProtocol tprotocol = new TBinaryProtocol(header);
                    thriftClient = (T) constructor.newInstance(tprotocol);
                    framed.open(); // 同步客户端需要手动打开（DubboHeaderClientTransport.open 委托给 framed）
                    System.out.println("Thrift client opened for " + url);
//                    T proxy = (T) ByteBuddyUtils.getProxy(syncIfaceClass, asyncIfaceClass, thriftClient);
//                    return proxy;
                    return thriftClient;
                } catch (Exception e) {
                    throw new RpcException("Fail to create remote client: " + url + "\n" + e.getMessage(), e);
                }
            }
//            else if (typeName.endsWith(THRIFT_ASYNC_IFACE)) {
//
//                String clientClsName = typeName.substring(0, idx) + THRIFT_ASYNC_CLIENT;
//                Class<?> clazz = Class.forName(clientClsName);
//                Constructor constructor = clazz.getConstructor(
//                        TProtocolFactory.class, TAsyncClientManager.class, TNonblockingTransport.class);
//                try {
//                    TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
//
//                    TAsyncClientManager clientManager = new TAsyncClientManager();
//
//                    // 异步客户端会自动open.因此不需要显示调用open
//                    TNonblockingSocket tSocket = new TNonblockingSocket(url.getHost(), url.getPort());
//
//                    // 这里还可以对tSocket设置项
//                    tSocket.setTimeout(1000 * 60); // 60秒超时
//
//                    thriftClient = (T) constructor.newInstance(protocolFactory, clientManager, tSocket);
//
//                    System.out.println("Thrift Async client opened for " + url);
//
//                    T proxy = (T) ByteBuddyUtils.getProxy(asyncIfaceClass, syncIfaceClass, thriftClient);
//                    return proxy;
//                } catch (Exception e) {
//                    throw new RpcException("Fail to create remote client: " + url + "\n" + e.getMessage(), e);
//                }
//            }
            else {
                throw new RpcException(String.format("%s is not a thrift interface", typeName));
            }

        } catch (Exception e) {
            throw new RpcException(e.getMessage());
        }
    }

    static class ThriftServer {
        THsHaServer server;
        Runnable runnable;
    }
}
