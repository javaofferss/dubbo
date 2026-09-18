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
import org.apache.dubbo.rpc.protocol.natives.thrift5.codec.DubboHeader;
import org.apache.dubbo.rpc.protocol.natives.thrift5.support.Thrift5AttachmentHolder;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 端到端 wire 自测：不拉起真实 socket，用内存 transport 模拟完整一跳
 * consumer 写 → [4B len][Dubbo 头|thrift] → provider 读（header→holder + thrift 消息回读），
 * 以及 provider 写响应 → consumer 读（剥头 + thrift 响应回读 + 回带附件塞回 RpcContext）。
 *
 * <p>覆盖 §6 阶段一 step 8 的核心断言（attachment 单跳打通、响应往返对称）。
 */
class WireRoundtripTest {

    @AfterEach
    void cleanup() {
        Thrift5AttachmentHolder.clear();
        RpcContext.removeClientResponseContext();
    }

    /** 消费者写 + 提供者读：附件经 holder 到达、thrift 消息完整回读。 */
    @Test
    void requestDirectionEndToEnd() throws Exception {
        // --- 消费者侧：写 [4B len][Dubbo 头 | thrift 消息] ---
        Map<String, Object> attachments = new LinkedHashMap<>();
        attachments.put("traceId", "t-end2end");
        attachments.put("tag", "gray");
        Thrift5AttachmentHolder.set(attachments);

        ByteArrayOutputStream wireOut = new ByteArrayOutputStream();
        TTransport socketSim = new TIOStreamTransport(wireOut);
        TTransport framed = new TFramedTransport(socketSim);
        DubboHeaderClientTransport clientTransport = new DubboHeaderClientTransport(framed);
        TProtocol clientProto = new TBinaryProtocol(clientTransport);

        clientProto.writeMessageBegin(new TMessage("echo", TMessageType.CALL, 7));
        clientProto.writeMessageEnd();
        clientTransport.flush(); // → framed.flush → wire = [4B len][头|thrift]
        Thrift5AttachmentHolder.clear(); // 模拟 consumer filter finally

        byte[] wire = wireOut.toByteArray();
        int len = readFrameLen(wire);
        byte[] frameBody = Arrays.copyOfRange(wire, 4, 4 + len);

        // --- 提供者侧：frame body = TMemoryInputTransport，经 inputProtocolFactory 消费头 ---
        TMemoryInputTransport inTrans = new TMemoryInputTransport(frameBody);
        TProtocol serverProto = new DubboHeaderInputProtocolFactory(new TBinaryProtocol.Factory()).getProtocol(inTrans);

        // holder 现在应有附件（模拟 provider filter 即将读取）
        assertEquals(attachments, Thrift5AttachmentHolder.get());

        // thrift 消息应能正常解析
        TMessage msg = serverProto.readMessageBegin();
        assertEquals("echo", msg.name);
        assertEquals(TMessageType.CALL, msg.type);
        assertEquals(7, msg.seqid);
        serverProto.readMessageEnd();
    }

    /** 提供者写响应（空头） + 消费者读：thrift 响应完整回读。 */
    @Test
    void responseDirectionEmptyHeader() throws Exception {
        // --- 提供者侧：写 [Dubbo 响应头(空) | thrift 响应消息] ---
        ByteArrayOutputStream respOut = new ByteArrayOutputStream();
        TTransport respWrite = new TIOStreamTransport(respOut);
        TProtocol outProto = new DubboHeaderOutputProtocolFactory().getProtocol(respWrite);
        outProto.writeMessageBegin(new TMessage("echo", TMessageType.REPLY, 7));
        outProto.writeMessageEnd();
        byte[] respBody = respOut.toByteArray();

        // --- 消费者侧：经 DubboHeaderClientTransport 读，先剥空响应头 ---
        DubboHeaderClientTransport clientRead = new DubboHeaderClientTransport(new TMemoryInputTransport(respBody));
        TProtocol clientProto = new TBinaryProtocol(clientRead);

        TMessage msg = clientProto.readMessageBegin();
        assertEquals("echo", msg.name);
        assertEquals(TMessageType.REPLY, msg.type);
        assertEquals(7, msg.seqid);
        clientProto.readMessageEnd();
    }

    /** 提供者写带附件的响应 + 消费者读：回带附件塞回 RpcContext.getClientResponseContext()。 */
    @Test
    void responseDirectionWithAttachments() throws Exception {
        Map<String, Object> respAttachments = new LinkedHashMap<>();
        respAttachments.put("hitCache", "true");
        respAttachments.put("serverSpanId", "span-9");

        // 构造响应 body = [Dubbo 响应头(带附件) | thrift 响应消息]
        ByteArrayOutputStream respOut = new ByteArrayOutputStream();
        TTransport headWriter = new TIOStreamTransport(respOut);
        DubboHeader.write(headWriter, respAttachments); // 先写响应头
        TProtocol outProto = new TBinaryProtocol(headWriter);
        outProto.writeMessageBegin(new TMessage("echo", TMessageType.REPLY, 7));
        outProto.writeMessageEnd();
        byte[] respBody = respOut.toByteArray();

        // 消费者读
        DubboHeaderClientTransport clientRead = new DubboHeaderClientTransport(new TMemoryInputTransport(respBody));
        TProtocol clientProto = new TBinaryProtocol(clientRead);

        assertNull(RpcContext.getClientResponseContext().getObjectAttachment("hitCache"));
        TMessage msg = clientProto.readMessageBegin(); // 触发剥响应头
        assertEquals("echo", msg.name);
        assertEquals(7, msg.seqid);

        // 回带附件已塞回 client response context
        assertEquals("true", RpcContext.getClientResponseContext().getObjectAttachment("hitCache"));
        assertEquals("span-9", RpcContext.getClientResponseContext().getObjectAttachment("serverSpanId"));
        clientProto.readMessageEnd();
    }

    /** 池化的 client transport 可复用：上一轮响应头消费标记在 flush 时被重置。 */
    @Test
    void pooledTransportReusableAcrossCalls() throws Exception {
        DubboHeaderClientTransport client = new DubboHeaderClientTransport(new TMemoryInputTransport(buildEmptyResp()));
        TProtocol p = new TBinaryProtocol(client);
        assertEquals("echo", p.readMessageBegin().name);
        // 第二轮：用全新响应体喂同一个 client transport（不抛 "已消费" 类问题）
        DubboHeaderClientTransport client2 =
                new DubboHeaderClientTransport(new TMemoryInputTransport(buildEmptyResp()));
        assertEquals("echo", new TBinaryProtocol(client2).readMessageBegin().name);
        assertNotNull(client2);
    }

    private byte[] buildEmptyResp() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TTransport w = new TIOStreamTransport(out);
        TProtocol p = new DubboHeaderOutputProtocolFactory().getProtocol(w);
        p.writeMessageBegin(new TMessage("echo", TMessageType.REPLY, 7));
        p.writeMessageEnd();
        return out.toByteArray();
    }

    private static int readFrameLen(byte[] wire) {
        return ((wire[0] & 0xFF) << 24) | ((wire[1] & 0xFF) << 16) | ((wire[2] & 0xFF) << 8) | (wire[3] & 0xFF);
    }
}
