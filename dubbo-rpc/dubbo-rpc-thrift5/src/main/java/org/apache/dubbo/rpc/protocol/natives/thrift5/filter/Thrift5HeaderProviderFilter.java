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

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcContextAttachment;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.protocol.natives.thrift5.Thrift5Protocol;
import org.apache.dubbo.rpc.protocol.natives.thrift5.support.Thrift5AttachmentHolder;
import org.apache.dubbo.rpc.protocol.natives.thrift5.support.Thrift5ResponseAttachmentHolder;

import java.util.Map;

/**
 * 提供者侧 attachment 桥接 filter（见 protocol-header-design.md §3.4）。
 *
 * <p>{@code @Activate(group=PROVIDER, order=Integer.MIN_VALUE+10)} —— 夹在
 * {@code ContextFilter}({@code MIN_VALUE}) 与 {@code ObservationReceiverFilter}({@code MIN_VALUE+50}) 之间：
 * <ol>
 *   <li>{@code ContextFilter} 先跑：此时 invocation 无附件（附件还在 holder 里、未注入），strip 无影响；建好 RpcContext。</li>
 *   <li>本 filter：holder → 注回 {@code invocation}（让后续 tracing receiver/token/echo 等从 invocation 读到）
 *       + {@code RpcContext.getServerAttachment()}（让业务方法从 RpcContext 读到）。</li>
 *   <li>{@code ObservationReceiverFilter} 后跑：从 invocation 抽 traceId。</li>
 * </ol>
 *
 * <p>此顺序使全链路灰度 tag 能重新进入 server RpcContext、绕过 ContextFilter 的 TAG_KEY strip，
 * 由原生 {@code ConsumerContextFilter} 在下一跳路由前穿透（详见 §5.3）。
 *
 * <p>非 thrift5 协议直接放行（{@link Thrift5Protocol#NAME} 校验）。
 */
@Activate(group = CommonConstants.PROVIDER, order = Integer.MIN_VALUE + 10)
public class Thrift5HeaderProviderFilter implements Filter, Filter.Listener {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation inv) throws RpcException {
        if (!Thrift5Protocol.NAME.equals(invoker.getUrl().getProtocol())) {
            return invoker.invoke(inv);
        }

        Map<String, Object> hdr = Thrift5AttachmentHolder.get();
        if (hdr != null && !hdr.isEmpty()) {
            // 1) 注回 invocation：让后续 ObservationReceiverFilter / TokenFilter / EchoFilter 等从 invocation 读到
            hdr.forEach(inv::setObjectAttachment);
            // 2) 同时塞 RpcContext.getServerAttachment()：业务方法从 RpcContext 读（ContextFilter 已先跑过、不会再 sync）
            RpcContextAttachment serverCtx = RpcContext.getServerAttachment();
            hdr.forEach(serverCtx::setObjectAttachment);
        }

        try {
            return invoker.invoke(inv);
        } finally {
            // 工作线程复用，必须清，否则脏附件泄漏到下一请求
            Thrift5AttachmentHolder.clear();
            // 防御：响应 holder 正常路径在 onResponse 才写，此处一般为空；异常路径兜底
            Thrift5ResponseAttachmentHolder.clear();
        }
    }

    /**
     * 响应方向附件捕获（阶段二）。{@code CallbackRegistrationInvoker} 反向序触发，本 filter（order
     * {@code MIN_VALUE+10}）的 {@code onResponse} 先于 {@code ContextFilter}（{@code MIN_VALUE}）触发——
     * 此时 {@code RpcContext.getServerResponseContext()} 仍被内层 filter/业务方法填充且尚未被
     * {@code ContextFilter.onResponse} 的 {@code removeServerResponseContext()} 清空，故在此快照进 holder，
     * 随后由 {@code DubboHeaderOutputProtocolFactory.writeMessageBegin} 读出写进响应头。
     */
    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        if (!Thrift5Protocol.NAME.equals(invoker.getUrl().getProtocol())) {
            return;
        }
        Map<String, Object> resp = RpcContext.getServerResponseContext().getObjectAttachments();
        if (resp != null && !resp.isEmpty()) {
            Thrift5ResponseAttachmentHolder.set(resp);
        }
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        if (!Thrift5Protocol.NAME.equals(invoker.getUrl().getProtocol())) {
            return;
        }
        // 异常响应不带附件，output factory 走 writeEmpty
        Thrift5ResponseAttachmentHolder.clear();
    }
}
