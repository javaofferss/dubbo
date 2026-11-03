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
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.protocol.natives.thrift5.Thrift5Protocol;
import org.apache.dubbo.rpc.protocol.natives.thrift5.support.Thrift5AttachmentHolder;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 消费者侧 attachment 桥接 filter（见 protocol-header-design.md §3.4）。
 *
 * <p>{@code @Activate(group=CONSUMER, order=Integer.MAX_VALUE-1000)} —— 最内层，
 * 在 {@code ObservationSenderFilter}({@code MIN_VALUE+50}) 等外层 filter 注入之后执行，
 * 捕获 {@code invocation.attachments ∪ RpcContext.getClientAttachment()} 灌入 {@link Thrift5AttachmentHolder}，
 * 供 {@code DubboHeaderClientTransport.flush()} 读出写进请求头。
 *
 * <p>非 thrift5 协议直接放行（{@link Thrift5Protocol#NAME} 校验），避免污染其它协议消费者。
 */
@Activate(group = CommonConstants.CONSUMER, order = Integer.MAX_VALUE - 1000)
public class Thrift5HeaderConsumerFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation inv) throws RpcException {
        if (!Thrift5Protocol.NAME.equals(invoker.getUrl().getProtocol())) {
            return invoker.invoke(inv);
        }

        Map<String, Object> hdr = new LinkedHashMap<>();
        Map<String, Object> invAtt = inv.getObjectAttachments();
        if (invAtt != null) {
            hdr.putAll(invAtt); // tracing/token 等外层 filter 注入的在此
        }
        Map<String, Object> clientAtt = RpcContext.getClientAttachment().getObjectAttachments();
        if (clientAtt != null && !clientAtt.isEmpty()) {
            hdr.putAll(clientAtt); // 用户直接 RpcContext.setAttachment 的在此
        }

        Thrift5AttachmentHolder.set(hdr);
        try {
            // → AbstractInvoker.doInvoke → ThriftPoolDirectProxy → thriftClient.send_X →
            // DubboHeaderClientTransport.flush()
            return invoker.invoke(inv);
        } finally {
            Thrift5AttachmentHolder.clear();
        }
    }
}
