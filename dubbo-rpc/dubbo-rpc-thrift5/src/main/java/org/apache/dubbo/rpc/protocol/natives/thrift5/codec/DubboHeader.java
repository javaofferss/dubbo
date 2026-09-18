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
package org.apache.dubbo.rpc.protocol.natives.thrift5.codec;

import java.util.Map;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * 编解码 thrift5 envelope 的 Dubbo 头（见 protocol-header-design.md §2.2）。
 *
 * <pre>
 * 偏移 长度 字段
 * 0    2    magic 0xD0 0xBB
 * 2    1    version/flags，低 4 位 = 附件序列化 id（2 = hessian2，与原生 dubbo 协议附件路径一致）
 * 3    2    headerLen（大端，附件块字节数，不含前 5 字节，上限 64KB）
 * 5    hl   附件块（由 {@link AttachmentCodec} 按 serialization id 编解码）
 * </pre>
 *
 * <p>本类只负责头的读写与 magic/序列化 id 校验，附件块编解码委托 {@link AttachmentCodec}。
 */
public final class DubboHeader {

    public static final byte MAGIC_HI = (byte) 0xD0;
    public static final byte MAGIC_LO = (byte) 0xBB;

    /**
     * serialization id = 2：hessian2，与原生 dubbo 协议附件编解码路径一致（{@code writeObject(Map)} / {@code readObject(Map.class)}），
     * 保留 Object 类型（Integer/Long/Boolean/null/嵌套 Map 等）。
     */
    public static final byte SER_ID_HESSIAN2 = 2;

    /** headerLen 用 2 字节，附件块上限 64KB。 */
    public static final int MAX_HEADER_LEN = 0xFFFF;

    private DubboHeader() {}

    /**
     * 写空头（headerLen=0）。阶段一响应方向与"无附件"请求方向使用。
     */
    public static void writeEmpty(TTransport trans) throws TTransportException {
        trans.write(EMPTY_HEADER, 0, 5);
    }

    /**
     * 写带附件的头。null / 空 map 走 {@link #writeEmpty}（headerLen=0），省一次 hessian2 序列化。
     *
     * @throws TTransportException 附件块超过 {@link #MAX_HEADER_LEN} 时抛出。
     */
    public static void write(TTransport trans, Map<String, Object> attachments) throws TTransportException {
        if (attachments == null || attachments.isEmpty()) {
            writeEmpty(trans);
            return;
        }
        byte[] body = AttachmentCodec.encode(attachments);
        if (body.length > MAX_HEADER_LEN) {
            throw new TTransportException("thrift5 dubbo header too large: " + body.length + " > " + MAX_HEADER_LEN
                    + ", consider trimming attachments");
        }
        byte[] head = new byte[5];
        head[0] = MAGIC_HI;
        head[1] = MAGIC_LO;
        head[2] = SER_ID_HESSIAN2;
        head[3] = (byte) ((body.length >> 8) & 0xFF);
        head[4] = (byte) (body.length & 0xFF);
        trans.write(head, 0, 5);
        trans.write(body, 0, body.length);
    }

    /**
     * 从 transport 读出并解析头，返回附件 map（无附件则返回空 map，永不为 null）。
     *
     * @throws TTransportException magic 不符或序列化 id 不支持时抛出。
     */
    public static Map<String, Object> read(TTransport trans) throws TTransportException {
        byte[] head = new byte[5];
        trans.readAll(head, 0, 5);
        if (head[0] != MAGIC_HI || head[1] != MAGIC_LO) {
            throw new TTransportException(
                    "peer is not a dubbo-thrift5 endpoint (bad magic), " + "native thrift interop is not supported");
        }
        byte serId = head[2];
        if (serId != SER_ID_HESSIAN2) {
            throw new TTransportException("unsupported thrift5 header serialization id: " + serId
                    + " (expected hessian2=" + SER_ID_HESSIAN2 + ")");
        }
        int hl = ((head[3] & 0xFF) << 8) | (head[4] & 0xFF);
        byte[] body = new byte[hl];
        if (hl > 0) {
            trans.readAll(body, 0, hl);
        }
        return AttachmentCodec.decode(body);
    }

    private static final byte[] EMPTY_HEADER = new byte[] {MAGIC_HI, MAGIC_LO, SER_ID_HESSIAN2, 0, 0};
}
