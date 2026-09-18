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
package org.apache.dubbo.rpc.protocol.natives.thrift5;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.StubMethod;
import net.bytebuddy.matcher.ElementMatchers;

/**
 * create by cmj
 */
public class ByteBuddyUtils {

    /**
     * 字节码生成. mainClass生效调用
     * @param mainClass
     * @param visClass
     * @param thriftClient
     * @return
     * @param <T>
     * @throws Exception
     */
    public static <T> T getProxy(Class<?> mainClass, Class<?> visClass, T thriftClient) throws Exception {
        // 生成代理对象目的是为了支持让实例对象属于type的子类
        Class<?> proxyClass = new ByteBuddy()
                .subclass(Object.class)
                .implement(mainClass, visClass)
                // 接口mainClass的方法转发给target
                .method(ElementMatchers.isDeclaredBy(mainClass))
                .intercept(MethodDelegation.to(thriftClient))
                // 接口visClass的方法返回默认值（null/0/false等）
                .method(ElementMatchers.isDeclaredBy(visClass))
                .intercept(StubMethod.INSTANCE) // 返回默认值
                .make()
                .load(thriftClient.getClass().getClassLoader())
                .getLoaded();

        T proxy = (T) proxyClass.getDeclaredConstructor().newInstance();
        return proxy;
    }
}
