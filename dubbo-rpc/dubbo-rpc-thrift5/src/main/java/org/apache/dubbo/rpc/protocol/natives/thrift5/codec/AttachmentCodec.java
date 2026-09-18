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

import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.common.serialize.hessian2.Hessian2ObjectInput;
import org.apache.dubbo.common.serialize.hessian2.Hessian2ObjectOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 附件块编解码（serialization id = 2，hessian2）。
 *
 * <p>与原生 dubbo 协议的附件编解码路径一致：{@code ObjectOutput.writeAttachments(map)} 默认实现即
 * {@code writeObject(map)}，{@code ObjectInput.readAttachments()} 默认实现即 {@code readObject(Map.class)}。
 * 因此 {@link Integer}/{@link Long}/{@link Boolean} 等基本类型、{@code null}、嵌套 {@link Map} 均按原类型往返，
 * 不做 String 扁平化——这正是用户在原生 dubbo 协议下得到的行为。
 *
 * <p>空 / null map 返回长度 0 的数组（对应 DubboHeader.writeEmpty 的 headerLen=0 路径），
 * 读侧长度 0 的 body 返回空 map，永不为 null。
 */
public final class AttachmentCodec {

    private AttachmentCodec() {}

    /**
     * 编码。null 或空 map 返回长度 0 的数组。
     *
     * @throws RuntimeException 包装 hessian2 写入时的 {@link java.io.IOException}（协议头编解码不抛 checked）。
     */
    public static byte[] encode(Map<String, Object> attachments) {
        if (attachments == null || attachments.isEmpty()) {
            return new byte[0];
        }
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(estimateSize(attachments));
            ObjectOutput out = new Hessian2ObjectOutput(baos);
            out.writeAttachments(attachments); // → writeObject(map)
            out.flushBuffer();
            return baos.toByteArray();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to encode thrift5 attachments (hessian2): " + e.getMessage(), e);
        }
    }

    /**
     * 解码。null 或空 body 返回空 map（有序、可变），永不为 null。
     *
     * @throws RuntimeException 包装 hessian2 读取时的 IO/ClassNotFound（协议异常由调用方收口）。
     */
    public static Map<String, Object> decode(byte[] body) {
        Map<String, Object> map = new LinkedHashMap<>();
        if (body == null || body.length == 0) {
            return map;
        }
        try {
            ObjectInput in = new Hessian2ObjectInput(new ByteArrayInputStream(body));
            Map<String, Object> decoded = in.readAttachments(); // → readObject(Map.class)
            if (decoded != null) {
                map.putAll(decoded);
            }
            return map;
        } catch (java.io.IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to decode thrift5 attachments (hessian2): " + e.getMessage(), e);
        }
    }

    private static int estimateSize(Map<String, Object> attachments) {
        int n = 0;
        for (Map.Entry<String, Object> e : attachments.entrySet()) {
            if (e.getKey() != null) {
                n += e.getKey().length() * 3; // UTF-8 上界估算
            }
            Object v = e.getValue();
            if (v instanceof String) {
                n += ((String) v).length() * 3;
            } else {
                n += 64; // 非字符串值粗略预留
            }
        }
        return Math.max(64, n);
    }
}
