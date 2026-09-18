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

import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcContextAttachment;
import org.apache.dubbo.rpc.protocol.natives.thrift5.codec.DubboHeader;
import org.apache.dubbo.rpc.protocol.natives.thrift5.support.Thrift5AttachmentHolder;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * 消费端 Transport 装饰器（见 protocol-header-design.md §3.2）。
 *
 * <p>它装饰同步客户端的 {@code TFramedTransport}：
 * <ul>
 *   <li>写路径：{@code TBinaryProtocol} 写 thrift 消息 → {@link #write} 缓冲到本地 BAOS；
 *       {@link #flush} 时先从 {@link Thrift5AttachmentHolder} 取附件写 Dubbo 请求头，
 *       再写缓冲的 thrift 字节，最后委托内层 {@code TFramedTransport.flush()}（自动补 4 字节 frame 长度）。
 *       最终 wire = {@code [4B len][Dubbo 头 | thrift 消息]}。</li>
 *   <li>读路径：{@link #read} 第一次调用时先消费响应 Dubbo 头，把回带附件塞回
 *       {@link RpcContext#getClientResponseContext()}，后续读透传给内层 transport 读 thrift 响应消息。</li>
 * </ul>
 *
 * <p>实例随 thrift client 一起池化（{@code ThriftGenericKeyedObjectPool}），故
 * {@link #flush} 会重置响应头消费标记以适配下一次调用。
 */
public class DubboHeaderClientTransport extends TTransport {

    private final TTransport inner;
    private final ByteArrayOutputStream writeBuffer = new ByteArrayOutputStream();

    /** 响应头是否已消费。实例池化，每次 flush 时重置。 */
    private boolean responseHeaderConsumed = false;

    public DubboHeaderClientTransport(TTransport inner) {
        this.inner = inner;
    }

    @Override
    public boolean isOpen() {
        return inner.isOpen();
    }

    @Override
    public void open() throws TTransportException {
        inner.open();
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException {
        if (!responseHeaderConsumed) {
            consumeResponseHeader();
            responseHeaderConsumed = true;
        }
        return inner.read(buf, off, len);
    }

    private void consumeResponseHeader() throws TTransportException {
        Map<String, Object> resp = DubboHeader.read(inner);
        if (resp != null && !resp.isEmpty()) {
            RpcContextAttachment ctx = RpcContext.getClientResponseContext();
            for (Map.Entry<String, Object> e : resp.entrySet()) {
                ctx.setObjectAttachment(e.getKey(), e.getValue());
            }
        }
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
        writeBuffer.write(buf, off, len);
    }

    @Override
    public void flush() throws TTransportException {
        // 新一轮请求-响应：重置响应头消费标记
        responseHeaderConsumed = false;

        Map<String, Object> hdr = Thrift5AttachmentHolder.get();
        if (hdr == null || hdr.isEmpty()) {
            DubboHeader.writeEmpty(inner);
        } else {
            DubboHeader.write(inner, hdr);
        }

        byte[] thriftBytes = writeBuffer.toByteArray();
        writeBuffer.reset();
        if (thriftBytes.length > 0) {
            inner.write(thriftBytes, 0, thriftBytes.length);
        }
        // 内层 TFramedTransport.flush(): 补 4 字节 frame 长度 → wire = [4B len][Dubbo头|thrift]
        inner.flush();
    }
}
