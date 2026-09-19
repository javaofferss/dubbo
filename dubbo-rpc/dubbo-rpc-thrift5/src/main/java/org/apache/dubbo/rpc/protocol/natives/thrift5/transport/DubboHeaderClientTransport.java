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

import java.util.Map;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * 消费端 Transport 装饰器（见 protocol-header-design.md §3.2）。
 *
 * <p>它装饰同步客户端的 {@code TFramedTransport}：
 * <ul>
 *   <li>写路径：不再自建 BAOS 中转。第一次 {@link #write} 时把 Dubbo 请求头（来自
 *       {@link Thrift5AttachmentHolder}）直接 {@code inner.write} 进 TFramedTransport 的 buffer，
 *       随后 thrift 字节也直接 {@code inner.write} 落进同一个 buffer。最终 inner 的 buffer 内为
 *       {@code [Dubbo 头 | thrift 消息]}，{@link #flush} 只需 {@code inner.flush()}
 *       （TFramedTransport 自动补 4 字节 frame 长度 → wire = {@code [4B len][Dubbo 头 | thrift 消息]}）。
 *       拷贝次数落到 TFramedTransport 帧协议下限（写进 buffer 1 次 + flush 的 toByteArray 1 次）。<br>
 *       {@code Thrsift5AttachmentHolder} 由 consumer filter 在 {@code invoker.invoke} 之前设好，
 *       早于 thrift client 的 {@code send_X}（即第一次 {@code write}），故第一次 write 时 header 必然就位。</li>
 *   <li>读路径：{@link #read} 第一次调用时先消费响应 Dubbo 头，把回带附件塞回
 *       {@link RpcContext#getClientResponseContext()}，后续读透传给内层 transport 读 thrift 响应消息。</li>
 * </ul>
 *
 * <p>实例随 thrift client 一起池化（{@code ThriftGenericKeyedObjectPool}），故
 * {@link #flush} 会重置响应头消费标记与 headerWritten 标记以适配下一次调用。
 */
public class DubboHeaderClientTransport extends TTransport {

    private final TTransport inner;

    /** 请求头是否已随本次写流写入 inner。实例池化，每次 flush 时重置。 */
    private volatile boolean headerWritten = false;

    /** 响应头是否已消费。实例池化，每次 flush 时重置。 */
    private volatile boolean responseHeaderConsumed = false;

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
        // 懒写：第一次写时先把 Dubbo 请求头落进 inner 的 buffer，保证 [头 | thrift 消息] 顺序
        if (!headerWritten) {
            writeRequestHeader();
            headerWritten = true;
        }
        inner.write(buf, off, len);
    }

    private void writeRequestHeader() throws TTransportException {
        Map<String, Object> hdr = Thrift5AttachmentHolder.get();
        if (hdr == null || hdr.isEmpty()) {
            DubboHeader.writeEmpty(inner);
        } else {
            DubboHeader.write(inner, hdr);
        }
    }

    @Override
    public void flush() throws TTransportException {
        // 新一轮请求-响应：重置两个标记以适配池化复用
        responseHeaderConsumed = false;

        // 兜底：若上层在未触发任何 write 的情况下直接 flush（理论上 thrift 不该如此），仍要出请求头
        if (!headerWritten) {
            writeRequestHeader();
            headerWritten = true;
        }
        headerWritten = false;

        // inner(TFramedTransport).flush(): 补 4 字节 frame 长度 → wire = [4B len][Dubbo头 | thrift]
        inner.flush();
    }
}
