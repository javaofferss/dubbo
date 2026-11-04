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
package org.apache.dubbo.rpc.cluster.filter.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcInvocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.apache.dubbo.common.constants.CommonConstants.AUTO_TAG_ARG_INDEX_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.AUTO_TAG_FORCE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.AUTO_TAG_FROM_ARG_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TAG_KEY;
import static org.apache.dubbo.rpc.Constants.FORCE_USE_TAG;

class AutoTagClusterFilterTest {

    private static final String FORCE_TRUE = "true";

    private AutoTagClusterFilter filter = new AutoTagClusterFilter();

    private RpcInvocation invocation(String method, Object... args) {
        Class<?>[] paramTypes = new Class<?>[args.length];
        for (int i = 0; i < args.length; i++) {
            paramTypes[i] = args[i] == null ? Object.class : args[i].getClass();
        }
        return new RpcInvocation(null, method, "demo.DemoService", "demo.DemoService", paramTypes, args);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Invoker invoker(URL url, Invocation inv) {
        Invoker invoker = Mockito.mock(Invoker.class);
        Mockito.when(invoker.getUrl()).thenReturn(url);
        Mockito.when(invoker.invoke(inv)).thenReturn(Mockito.mock(Result.class));
        return invoker;
    }

    private URL url(boolean enabled, int index, boolean force) {
        return URL.valueOf("dubbo://127.0.0.1:20880/demo.DemoService?" + AUTO_TAG_FROM_ARG_KEY + "=" + enabled
                + "&" + AUTO_TAG_ARG_INDEX_KEY + "=" + index + "&" + AUTO_TAG_FORCE_KEY + "="
                + force);
    }

    @Test
    void enabledInjectsTagAndForce() {
        URL url = url(true, 0, true);
        RpcInvocation inv = invocation("sayHello", "tenantA");
        filter.invoke(invoker(url, inv), inv);

        Assertions.assertEquals("tenantA", inv.getAttachment(TAG_KEY));
        Assertions.assertEquals(FORCE_TRUE, inv.getAttachment(FORCE_USE_TAG));
    }

    @Test
    void disabledDoesNotInject() {
        URL url = url(false, 0, true);
        RpcInvocation inv = invocation("sayHello", "tenantA");
        filter.invoke(invoker(url, inv), inv);

        Assertions.assertNull(inv.getAttachment(TAG_KEY));
        Assertions.assertNull(inv.getAttachment(FORCE_USE_TAG));
    }

    @Test
    void explicitTagIsNotOverridden() {
        URL url = url(true, 0, true);
        RpcInvocation inv = invocation("sayHello", "tenantA");
        inv.setAttachment(TAG_KEY, "manual");
        filter.invoke(invoker(url, inv), inv);

        Assertions.assertEquals("manual", inv.getAttachment(TAG_KEY));
        // force is not written either, since we returned before reaching the injection block
        Assertions.assertNull(inv.getAttachment(FORCE_USE_TAG));
    }

    @Test
    void nullArgSkipsInjection() {
        URL url = url(true, 0, true);
        RpcInvocation inv = invocation("sayHello", new Object[] {null});
        filter.invoke(invoker(url, inv), inv);

        Assertions.assertNull(inv.getAttachment(TAG_KEY));
    }

    @Test
    void indexOutOfRangeSkipsInjection() {
        URL url = url(true, 5, true);
        RpcInvocation inv = invocation("sayHello", "tenantA");
        filter.invoke(invoker(url, inv), inv);

        Assertions.assertNull(inv.getAttachment(TAG_KEY));
    }

    @Test
    void nonStringArgConvertedToString() {
        URL url = url(true, 0, true);
        RpcInvocation inv = invocation("sayHello", 1001L);
        filter.invoke(invoker(url, inv), inv);

        Assertions.assertEquals("1001", inv.getAttachment(TAG_KEY));
    }

    @Test
    void forceFalsePropagated() {
        URL url = url(true, 0, false);
        RpcInvocation inv = invocation("sayHello", "tenantA");
        filter.invoke(invoker(url, inv), inv);

        Assertions.assertEquals("tenantA", inv.getAttachment(TAG_KEY));
        Assertions.assertEquals("false", inv.getAttachment(FORCE_USE_TAG));
    }

    @Test
    void invokesDelegateExactlyOnce() {
        URL url = url(true, 0, true);
        RpcInvocation inv = invocation("sayHello", "tenantA");
        Invoker invoker = invoker(url, inv);
        filter.invoke(invoker, inv);
        Mockito.verify(invoker, Mockito.times(1)).invoke(inv);
    }

    @Test
    void indexOneUsesSecondArgument() {
        URL url = url(true, 1, true);
        RpcInvocation inv = invocation("create", "ignored", "zoneB");
        filter.invoke(invoker(url, inv), inv);

        Assertions.assertEquals("zoneB", inv.getAttachment(TAG_KEY));
    }

    @Test
    void emptyArgumentsSkipsInjection() {
        URL url = url(true, 0, true);
        RpcInvocation inv = invocation("noArgs");
        filter.invoke(invoker(url, inv), inv);

        Assertions.assertNull(inv.getAttachment(TAG_KEY));
    }
}
