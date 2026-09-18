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
package org.apache.dubbo.rpc.protocol.natives.thrift5.transport;

import org.apache.dubbo.rpc.protocol.natives.thrift5.codec.DubboHeader;
import org.apache.dubbo.rpc.protocol.natives.thrift5.support.Thrift5AttachmentHolder;

import java.util.Map;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;

/**
 * 提供端请求方向 hook（见 protocol-header-design.md §3.3）。
 *
 * <p>{@link TNonblockingServer.FrameBuffer#invoke()} 在工作线程调用
 * {@code inputProtocolFactory_.getProtocol(inTrans)}，其中 {@code inTrans} 是
 * {@code new TMemoryInputTransport(buffer_.array())}（含完整 frame body = [Dubbo 头 | thrift 消息]）。
 *
 * <p>本 factory 在此 eager 消费 Dubbo 头、附件入 {@link Thrift5AttachmentHolder}，
 * 再把 {@code inTrans}（pos 已越过头）交给委托 {@link TProtocolFactory} 构造 {@link TProtocol} 正常解析 thrift 消息。
 *
 * <p>头解析发生在工作线程，与后续 processor → Dubbo 代理 → Filter 链同线程（§5 风险1 已实证）。
 */
public class DubboHeaderInputProtocolFactory implements TProtocolFactory {

    private static final long serialVersionUID = 0L;

    private final TProtocolFactory delegate;

    public DubboHeaderInputProtocolFactory(TProtocolFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    public TProtocol getProtocol(TTransport trans) {
        // 防御性清理上一请求残留（正常路径 provider filter 的 finally 已 clear）
        Thrift5AttachmentHolder.clear();
        Map<String, Object> hdr;
        try {
            hdr = DubboHeader.read(trans);
        } catch (org.apache.thrift.transport.TTransportException e) {
            // TProtocolFactory.getProtocol 不允许抛 checked 异常；FrameBuffer.invoke 会 catch(Exception) 收口
            throw new RuntimeException("Failed to read thrift5 dubbo header: " + e.getMessage(), e);
        }
        if (hdr != null && !hdr.isEmpty()) {
            Thrift5AttachmentHolder.set(hdr);
        }
        // inTrans 的 pos 已越过 Dubbo 头，剩余字节即 thrift 消息，交委托 factory 正常解析
        return delegate.getProtocol(trans);
    }
}
