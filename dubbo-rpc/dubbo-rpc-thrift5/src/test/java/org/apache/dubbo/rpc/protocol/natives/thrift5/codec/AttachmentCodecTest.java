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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AttachmentCodecTest {

    @Test
    void emptyEncodeDecode() {
        assertArrayEquals(new byte[0], AttachmentCodec.encode(null));
        assertArrayEquals(new byte[0], AttachmentCodec.encode(Collections.emptyMap()));
        assertTrue(AttachmentCodec.decode(null).isEmpty());
        assertTrue(AttachmentCodec.decode(new byte[0]).isEmpty());
    }

    @Test
    void roundtripMultiple() {
        Map<String, Object> src = new LinkedHashMap<>();
        src.put("traceId", "abc-123");
        src.put("tag", "gray");
        src.put("timeout", "3000");
        src.put("empty", "");

        byte[] body = AttachmentCodec.encode(src);
        Map<String, Object> dst = AttachmentCodec.decode(body);

        assertEquals(src, dst);
    }

    @Test
    void roundtripUnicode() {
        Map<String, Object> src = new LinkedHashMap<>();
        src.put("用户", "张三");
        src.put("标", "灰度-标签");

        Map<String, Object> dst = AttachmentCodec.decode(AttachmentCodec.encode(src));
        assertEquals(src, dst);
    }

    @Test
    void nullValueBecomesEmpty() {
        Map<String, Object> src = new LinkedHashMap<>();
        src.put("k", null);
        Map<String, Object> dst = AttachmentCodec.decode(AttachmentCodec.encode(src));
        assertEquals("", dst.get("k"));
    }

    @Test
    void nonStringValueStringified() {
        Map<String, Object> src = new LinkedHashMap<>();
        src.put("count", 42L);
        Map<String, Object> dst = AttachmentCodec.decode(AttachmentCodec.encode(src));
        assertEquals("42", dst.get("count"));
    }
}
