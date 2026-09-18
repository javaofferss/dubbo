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
package org.apache.dubbo.rpc.protocol.natives.thrift5.support;

import java.util.Map;

/**
 * Transport 与 Dubbo Filter 之间传递附件的 ThreadLocal（见 protocol-header-design.md §3.1）。
 *
 * <p>消费者侧：{@code Thrift5HeaderConsumerFilter} 在最内层 filter 把 {@code invocation ∪ RpcContext}
 * 灌入 holder，随后 {@code DubboHeaderClientTransport.flush()} 读出写进请求头。
 *
 * <p>提供者侧：{@code DubboHeaderInputProtocolFactory} 在工作线程 eager 读出请求头、灌入 holder，
 * 随后 {@code Thrift5HeaderProviderFilter} 读出 holder 注回 {@code invocation + RpcContext.getServerAttachment()}。
 *
 * <p>线程模型已实证（§5 风险1）：头解析 + holder.set + Filter 链 + 业务方法全在同一工作线程，
 * 普通 ThreadLocal 即可，无需 InheritableThreadLocal。
 */
public final class Thrift5AttachmentHolder {

    private static final ThreadLocal<Map<String, Object>> HOLDER = new ThreadLocal<>();

    private Thrift5AttachmentHolder() {}

    public static void set(Map<String, Object> attachments) {
        HOLDER.set(attachments);
    }

    public static Map<String, Object> get() {
        return HOLDER.get();
    }

    /** 工作线程复用，每个请求结束必须 clear，否则脏附件泄漏到下一请求。 */
    public static void clear() {
        HOLDER.remove();
    }
}
