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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 简单 KV 附件序列化（serialization id = 0）。
 *
 * <p>格式：重复 [4 字节 keyLen(大端)][key UTF-8][4 字节 valLen(大端)][val UTF-8]，直到消费完。
 *
 * <p>值非字符串时 toString；这是协议头治理附件的约定（扁平 String→String，键短、条目少）。
 */
public final class AttachmentCodec {

    private AttachmentCodec() {}

    /**
     * 编码。空或 null 返回长度 0 的数组。
     *
     * @throws IllegalArgumentException 单个 key/value 长度超过 {@link Integer#MAX_VALUE} 时（理论上限，实际被 headerLen 64KB 约束）。
     */
    public static byte[] encode(Map<String, Object> attachments) {
        if (attachments == null || attachments.isEmpty()) {
            return new byte[0];
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(256);
        for (Map.Entry<String, Object> e : attachments.entrySet()) {
            String k = e.getKey();
            if (k == null) {
                continue;
            }
            String v = e.getValue() == null ? "" : e.getValue().toString();
            byte[] kb = k.getBytes(StandardCharsets.UTF_8);
            byte[] vb = v.getBytes(StandardCharsets.UTF_8);
            writeInt32(out, kb.length);
            out.write(kb, 0, kb.length);
            writeInt32(out, vb.length);
            out.write(vb, 0, vb.length);
        }
        return out.toByteArray();
    }

    /**
     * 解码。null 或空返回空 map（有序、可变），永不为 null。
     *
     * @throws ArrayIndexOutOfBoundsException 字节流不完整（被截断）时抛出——属协议异常，由调用方收口。
     */
    public static Map<String, Object> decode(byte[] body) {
        Map<String, Object> map = new LinkedHashMap<>();
        if (body == null || body.length == 0) {
            return map;
        }
        int pos = 0;
        while (pos < body.length) {
            int kl = readInt32(body, pos);
            pos += 4;
            String k = new String(body, pos, kl, StandardCharsets.UTF_8);
            pos += kl;
            int vl = readInt32(body, pos);
            pos += 4;
            String v = new String(body, pos, vl, StandardCharsets.UTF_8);
            pos += vl;
            map.put(k, v);
        }
        return map;
    }

    private static void writeInt32(ByteArrayOutputStream out, int v) {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    private static int readInt32(byte[] b, int off) {
        return ((b[off] & 0xFF) << 24) | ((b[off + 1] & 0xFF) << 16) | ((b[off + 2] & 0xFF) << 8) | (b[off + 3] & 0xFF);
    }
}
