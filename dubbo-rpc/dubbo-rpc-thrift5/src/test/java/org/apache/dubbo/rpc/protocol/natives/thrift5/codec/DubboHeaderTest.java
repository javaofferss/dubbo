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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DubboHeaderTest {

    @Test
    void writeEmptyThenRead() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TTransport w = new TIOStreamTransport(out);
        DubboHeader.writeEmpty(w);

        TTransport r = new TMemoryInputTransport(out.toByteArray());
        assertTrue(DubboHeader.read(r).isEmpty());
    }

    @Test
    void writeWithAttachmentsThenRead() throws Exception {
        Map<String, Object> src = new LinkedHashMap<>();
        src.put("traceId", "t-1");
        src.put("tag", "gray");
        src.put("retries", 3); // 非字符串值：验证类型保留，不扁平成 "3"

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TTransport w = new TIOStreamTransport(out);
        DubboHeader.write(w, src);

        Map<String, Object> dst = DubboHeader.read(new TMemoryInputTransport(out.toByteArray()));
        assertEquals("t-1", dst.get("traceId"));
        assertEquals("gray", dst.get("tag"));
        assertEquals(Integer.valueOf(3), dst.get("retries"));
    }

    @Test
    void writeEmptyMapProducesEmptyHeader() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TTransport w = new TIOStreamTransport(out);
        DubboHeader.write(w, Collections.emptyMap());

        // 仅 5 字节定长头，headerLen=0
        assertEquals(5, out.size());
        byte[] b = out.toByteArray();
        assertEquals((byte) 0xD0, b[0]);
        assertEquals((byte) 0xBB, b[1]);
        assertEquals(DubboHeader.SER_ID_HESSIAN2, b[2]); // ser id = 2 (hessian2)
        assertEquals(0, b[3]); // hl hi
        assertEquals(0, b[4]); // hl lo
    }

    @Test
    void badMagicRejected() {
        byte[] bad = new byte[] {0x00, 0x00, 0x00, 0x00, 0x00};
        TTransport r = new TMemoryInputTransport(bad);
        TTransportException ex = assertThrows(TTransportException.class, () -> DubboHeader.read(r));
        assertTrue(ex.getMessage().contains("bad magic"));
    }

    @Test
    void unsupportedSerIdRejected() {
        byte[] bad = new byte[] {(byte) 0xD0, (byte) 0xBB, (byte) 0x01, 0x00, 0x00};
        TTransport r = new TMemoryInputTransport(bad);
        TTransportException ex = assertThrows(TTransportException.class, () -> DubboHeader.read(r));
        assertTrue(ex.getMessage().contains("serialization id"));
    }

    @Test
    void oversizeAttachmentsRejected() {
        Map<String, Object> big = new LinkedHashMap<>();
        StringBuilder sb = new StringBuilder(DubboHeader.MAX_HEADER_LEN + 16);
        for (int i = 0; i < DubboHeader.MAX_HEADER_LEN + 16; i++) {
            sb.append('x');
        }
        big.put("k", sb.toString());
        TTransport w = new TIOStreamTransport(new ByteArrayOutputStream());
        assertThrows(TTransportException.class, () -> DubboHeader.write(w, big));
    }
}
