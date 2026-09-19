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
package org.apache.dubbo.rpc.protocol.natives.thrift5.filter;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.protocol.natives.thrift5.support.Thrift5AttachmentHolder;
import org.apache.dubbo.rpc.protocol.natives.thrift5.support.Thrift5ResponseAttachmentHolder;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Filter 桥接逻辑自测（不拉起真实 thrift 调用）：
 * provider filter holder→invocation+serverRpcContext、consumer filter inv∪RpcContext→holder、
 * 非 thrift5 协议直接放行。
 */
class Thrift5HeaderFilterTest {

    @AfterEach
    void cleanup() {
        Thrift5AttachmentHolder.clear();
        Thrift5ResponseAttachmentHolder.clear();
        RpcContext.removeClientResponseContext();
        RpcContext.removeServerResponseContext();
    }

    @Test
    void providerFilterInjectsHolderToInvocationAndRpcContext() throws RpcException {
        Map<String, Object> hdr = new LinkedHashMap<>();
        hdr.put("traceId", "t-1");
        hdr.put("tag", "gray");
        Thrift5AttachmentHolder.set(hdr);

        RpcInvocation inv = new RpcInvocation();
        final Map<String, Object> capturedInvAtt = new LinkedHashMap<>();
        final Map<String, Object> capturedServerAtt = new LinkedHashMap<>();

        Invoker<Object> stub = new Invoker<Object>() {
            @Override
            public Class<Object> getInterface() {
                return Object.class;
            }

            @Override
            public URL getUrl() {
                return URL.valueOf("thrift5://localhost:40880/MyService");
            }

            @Override
            public boolean isAvailable() {
                return true;
            }

            @Override
            public Result invoke(Invocation invocation) throws RpcException {
                capturedInvAtt.putAll(invocation.getObjectAttachments());
                capturedServerAtt.putAll(RpcContext.getServerAttachment().getObjectAttachments());
                return AsyncRpcResult.newDefaultAsyncResult((Object) null, invocation);
            }

            @Override
            public void destroy() {}
        };

        new Thrift5HeaderProviderFilter().invoke(stub, inv);

        assertEquals("t-1", capturedInvAtt.get("traceId"));
        assertEquals("gray", capturedInvAtt.get("tag"));
        assertEquals("t-1", capturedServerAtt.get("traceId"));
        assertEquals("gray", capturedServerAtt.get("tag"));
        // finally 已 clear holder
        assertNull(Thrift5AttachmentHolder.get());
    }

    @Test
    void consumerFilterCapturesInvUnionRpcContextToHolder() throws RpcException {
        Thrift5AttachmentHolder.clear();

        RpcInvocation inv = new RpcInvocation();
        inv.setObjectAttachment("traceId", "t-inv"); // 模拟 tracing 外层 filter 注入
        RpcContext.getClientAttachment().setObjectAttachment("userTag", "gray"); // 模拟用户直接 set

        final Map<String, Object> capturedHolder = new LinkedHashMap<>();
        Invoker<Object> stub = new Invoker<Object>() {
            @Override
            public Class<Object> getInterface() {
                return Object.class;
            }

            @Override
            public URL getUrl() {
                return URL.valueOf("thrift5://localhost:40880/MyService");
            }

            @Override
            public boolean isAvailable() {
                return true;
            }

            @Override
            public Result invoke(Invocation invocation) throws RpcException {
                Map<String, Object> h = Thrift5AttachmentHolder.get();
                if (h != null) {
                    capturedHolder.putAll(h);
                }
                return AsyncRpcResult.newDefaultAsyncResult((Object) null, invocation);
            }

            @Override
            public void destroy() {}
        };

        new Thrift5HeaderConsumerFilter().invoke(stub, inv);

        // 两侧来源都进 holder
        assertEquals("t-inv", capturedHolder.get("traceId"));
        assertEquals("gray", capturedHolder.get("userTag"));
        // finally 已 clear holder
        assertNull(Thrift5AttachmentHolder.get());
    }

    @Test
    void nonThrift5ProtocolBypassed() throws RpcException {
        Thrift5AttachmentHolder.clear();
        RpcInvocation inv = new RpcInvocation();
        inv.setObjectAttachment("x", "y");

        Invoker<Object> stub = new Invoker<Object>() {
            @Override
            public Class<Object> getInterface() {
                return Object.class;
            }

            @Override
            public URL getUrl() {
                return URL.valueOf("dubbo://localhost:20880/OtherService");
            }

            @Override
            public boolean isAvailable() {
                return true;
            }

            @Override
            public Result invoke(Invocation invocation) throws RpcException {
                return AsyncRpcResult.newDefaultAsyncResult((Object) null, invocation);
            }

            @Override
            public void destroy() {}
        };

        new Thrift5HeaderConsumerFilter().invoke(stub, inv);
        new Thrift5HeaderProviderFilter().invoke(stub, inv);
        // 非 thrift5：不应碰 holder
        assertTrue(Thrift5AttachmentHolder.get() == null
                || Thrift5AttachmentHolder.get().isEmpty());
    }

    /** provider filter.onResponse 把 getServerResponseContext() 快照进响应 holder(先于 ContextFilter.onResponse 清空)。 */
    @Test
    void providerOnResponseCapturesServerResponseContextToHolder() {
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("hitCache", "true");
        resp.put("serverSpanId", "span-9");
        RpcContext.getServerResponseContext().getObjectAttachments().putAll(resp);

        RpcInvocation inv = new RpcInvocation();
        Result result = AsyncRpcResult.newDefaultAsyncResult((Object) null, inv);
        Invoker<Object> stub = thrift5Stub();

        new Thrift5HeaderProviderFilter().onResponse(result, stub, inv);

        assertEquals("true", Thrift5ResponseAttachmentHolder.get().get("hitCache"));
        assertEquals("span-9", Thrift5ResponseAttachmentHolder.get().get("serverSpanId"));
    }

    /** provider filter.onError 清响应 holder(异常响应不带附件,factory 走 writeEmpty)。 */
    @Test
    void providerOnErrorClearsHolder() {
        Map<String, Object> stale = new LinkedHashMap<>();
        stale.put("stale", "leak");
        Thrift5ResponseAttachmentHolder.set(stale);
        RpcInvocation inv = new RpcInvocation();
        Invoker<Object> stub = thrift5Stub();

        new Thrift5HeaderProviderFilter().onError(new RuntimeException("boom"), stub, inv);

        assertNull(Thrift5ResponseAttachmentHolder.get());
    }

    /** 非 thrift5 协议:onResponse 不碰响应 holder。 */
    @Test
    void providerOnResponseBypassedForNonThrift5() {
        Thrift5ResponseAttachmentHolder.clear();
        RpcContext.getServerResponseContext().setObjectAttachment("x", "y");
        RpcInvocation inv = new RpcInvocation();
        Result result = AsyncRpcResult.newDefaultAsyncResult((Object) null, inv);
        Invoker<Object> dubboStub = new Invoker<Object>() {
            @Override
            public Class<Object> getInterface() {
                return Object.class;
            }

            @Override
            public URL getUrl() {
                return URL.valueOf("dubbo://localhost:20880/OtherService");
            }

            @Override
            public boolean isAvailable() {
                return true;
            }

            @Override
            public Result invoke(Invocation invocation) throws RpcException {
                return AsyncRpcResult.newDefaultAsyncResult((Object) null, invocation);
            }

            @Override
            public void destroy() {}
        };

        new Thrift5HeaderProviderFilter().onResponse(result, dubboStub, inv);

        assertNull(Thrift5ResponseAttachmentHolder.get());
    }

    private static Invoker<Object> thrift5Stub() {
        return new Invoker<Object>() {
            @Override
            public Class<Object> getInterface() {
                return Object.class;
            }

            @Override
            public URL getUrl() {
                return URL.valueOf("thrift5://localhost:40880/MyService");
            }

            @Override
            public boolean isAvailable() {
                return true;
            }

            @Override
            public Result invoke(Invocation invocation) throws RpcException {
                return AsyncRpcResult.newDefaultAsyncResult((Object) null, invocation);
            }

            @Override
            public void destroy() {}
        };
    }
}
