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
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.protocol.AbstractProxyProtocol;
import org.apache.dubbo.rpc.protocol.natives.thrift5.transport.DubboHeaderClientTransport;
import org.apache.dubbo.rpc.protocol.natives.thrift5.transport.DubboHeaderInputProtocolFactory;
import org.apache.dubbo.rpc.protocol.natives.thrift5.transport.DubboHeaderOutputProtocolFactory;

import java.lang.reflect.Constructor;
import java.net.SocketException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.apache.thrift.TProcessor;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.*;

/**
 * 支持thrift序列化0.5版本
 */
public class Thrift5Protocol extends AbstractProxyProtocol {

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
            if (thriftServer.server.isServing()) {
                return thriftServer.runnable;
            }
            servers.remove(key);
        }

        servers.computeIfAbsent(key, k -> {
            ThriftServer server = processExport(url.getPort(), getThriftProcessor(impl, type));
            return server;
        });

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

    public THsHaServer.Args getDefaultArgs(
            TNonblockingServerTransport serverTransport, TProcessor processor, TProtocolFactory protocolFactory) {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = new ThreadPoolExecutor(
                cpuCores * 2, // 核心线程数（保持活跃）
                cpuCores * 4, // 最大线程数（应对突发流量）
                60L, // 空闲线程存活时间
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), // 无边界队列，可能内存溢出
                new ThreadFactory() {
                    private final ThreadGroup group = new ThreadGroup("Thrift-Workers");
                    private int counter = 0;

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(group, r, "worker-" + (counter++));
                        t.setDaemon(false); // 非守护线程，确保任务完成
                        return t;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy() // 拒绝策略：由调用者线程执行，保证系统不崩溃
                );

        // 5. 组装 THsHaServer 参数
        // inputProtocolFactory：请求方向 eager 消费 Dubbo 头 → holder
        // outputProtocolFactory：响应方向 writeMessageBegin 先写 Dubbo 响应头（阶段一空头）
        // transportFactory 默认 TFramedTransport.Factory（由 AbstractNonblockingServerArgs 构造），保持不变
        THsHaServer.Args args = new THsHaServer.Args(serverTransport)
                .processor(processor)
                .inputProtocolFactory(new DubboHeaderInputProtocolFactory(protocolFactory))
                .outputProtocolFactory(new DubboHeaderOutputProtocolFactory())
                .executorService(executorService);
        return args;
    }

    public ThriftServer processExport(int port, TProcessor processor) {
        try {
            ThriftServer thriftServer = new ThriftServer();
            // 1. 创建处理器
            //            TOrderService.Processor processor =
            //                    new TOrderService.Processor(new TOrderServiceImpl());

            // 2. 创建非阻塞ServerSocket，监听端口9090
            TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);

            // 3. 配置协议工厂，注意服务端和客户端的协议要保持一致
            TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();

            // 4. 创建非阻塞服务器参数
            THsHaServer.Args defaultArgs = getDefaultArgs(serverTransport, processor, protocolFactory);

            // 5. 创建非阻塞服务器
            TServer server = new THsHaServer(defaultArgs);

            // 6. 启动服务（会阻塞主线程）
            new Thread(() -> {
                        server.serve();
                    })
                    .start();

            System.out.println("\nThrift server started at port " + port);
            thriftServer.server = server;
            thriftServer.runnable = () -> {
                if (server.isServing()) {
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
            String path = url.getPath();

            int idx = Math.max(typeName.indexOf(THRIFT_ASYNC_IFACE), typeName.indexOf(THRIFT_IFACE));
            String asyncIfaceClassName = typeName.substring(0, idx) + THRIFT_ASYNC_IFACE;
            Class<?> asyncIfaceClass = Class.forName(asyncIfaceClassName);
            String syncIfaceClassName = typeName.substring(0, idx) + THRIFT_IFACE;
            Class<?> syncIfaceClass = Class.forName(syncIfaceClassName);
            if (path.endsWith(THRIFT_IFACE)) {
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
                    T proxy = (T) ByteBuddyUtils.getProxy(syncIfaceClass, asyncIfaceClass, thriftClient);
                    return proxy;
                } catch (Exception e) {
                    throw new RpcException("Fail to create remote client: " + url + "\n" + e.getMessage(), e);
                }
            } else if (path.endsWith(THRIFT_ASYNC_IFACE)) {

                String clientClsName = typeName.substring(0, idx) + THRIFT_ASYNC_CLIENT;
                Class<?> clazz = Class.forName(clientClsName);
                Constructor constructor = clazz.getConstructor(
                        TProtocolFactory.class, TAsyncClientManager.class, TNonblockingTransport.class);
                try {
                    TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();

                    TAsyncClientManager clientManager = new TAsyncClientManager();

                    // 异步客户端会自动open.因此不需要显示调用open
                    TNonblockingSocket tSocket = new TNonblockingSocket(url.getHost(), url.getPort());

                    // 这里还可以对tSocket设置项
                    tSocket.setTimeout(1000 * 60); // 60秒超时

                    thriftClient = (T) constructor.newInstance(protocolFactory, clientManager, tSocket);

                    System.out.println("Thrift Async client opened for " + url);

                    T proxy = (T) ByteBuddyUtils.getProxy(asyncIfaceClass, syncIfaceClass, thriftClient);
                    return proxy;
                } catch (Exception e) {
                    throw new RpcException("Fail to create remote client: " + url + "\n" + e.getMessage(), e);
                }
            } else {
                throw new RpcException(String.format("%s is not a thrift interface", typeName));
            }

        } catch (Exception e) {
            throw new RpcException(e.getMessage());
        }
    }

    static class ThriftServer {
        TServer server;
        Runnable runnable;
    }
}
