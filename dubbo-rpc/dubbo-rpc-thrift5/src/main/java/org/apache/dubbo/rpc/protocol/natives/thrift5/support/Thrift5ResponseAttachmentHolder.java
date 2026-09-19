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
 * 响应方向附件传递 ThreadLocal（阶段二，见 protocol-header-design.md §3.3/§3.4）。
 *
 * <p>与请求侧 {@link Thrift5AttachmentHolder} 对称，但方向相反：
 * <ul>
 *   <li>请求侧：{@code DubboHeaderInputProtocolFactory} 写 holder → {@code Thrift5HeaderProviderFilter.invoke} 读。</li>
 *   <li>响应侧：{@code Thrift5HeaderProviderFilter.onResponse} 写 holder →
 *       {@code DubboHeaderOutputProtocolFactory.writeMessageBegin} 读。</li>
 * </ul>
 *
 * <p>为何要 holder 而非让 output factory 直接读 {@code RpcContext.getServerResponseContext()}：
 * sync 路径下 {@code Filter.Listener.onResponse} 链在 {@code invoker.invoke} 内部同步触发，
 * {@code ContextFilter.onResponse}（order {@code MIN_VALUE}，反向序最末）在 thrift
 * {@code writeMessageBegin} <b>之前</b>就 {@code removeServerResponseContext()} 清空了
 * {@code SERVER_RESPONSE_LOCAL}。而本 filter 的 {@code onResponse}（order {@code MIN_VALUE+10}）
 * 先于 {@code ContextFilter} 触发——此时 {@code getServerResponseContext()} 仍在，故在此捕获。
 * 线程模型同请求侧：头读/写 + Filter 链 + thrift 写全在同一工作线程。
 */
public final class Thrift5ResponseAttachmentHolder {

    private static final ThreadLocal<Map<String, Object>> HOLDER = new ThreadLocal<>();

    private Thrift5ResponseAttachmentHolder() {}

    public static void set(Map<String, Object> attachments) {
        HOLDER.set(attachments);
    }

    public static Map<String, Object> get() {
        return HOLDER.get();
    }

    /** 工作线程复用，output factory 读后必须 clear，否则脏附件泄漏到下一请求。 */
    public static void clear() {
        HOLDER.remove();
    }
}
