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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.protocol.natives.thrift5.ThriftGenericKeyedObjectPool.EndPoint;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 资源池代理. 用jdk代理做测试
 */
public class ThriftPoolProxy implements InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(ThriftPoolProxy.class);
    /**
     * 资源池
     */
    ThriftGenericKeyedObjectPool pool;

    /**
     * 资源池服务的key
     */
    EndPoint key;

    AtomicInteger borrowCount = new AtomicInteger();

    AtomicInteger returnCount = new AtomicInteger();

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Exception {
        Object o = pool.borrowObject(key);
        try {
            borrowCount.incrementAndGet();
            return method.invoke(o, args);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            returnCount.incrementAndGet();
            pool.returnObject(key, o);
            log.info("borrowCount {}, returnCount {}", borrowCount.get(), returnCount.get());
        }
    }

    public static <T> T getProxyObject(Class clazz, URL url, BiFunction<Class, URL, Object> processRefer)
            throws Exception {
        Class<?>[] interfaces = null;
        if (clazz.isInterface()) {
            interfaces = new Class<?>[] {clazz};
        } else {
            interfaces = clazz.getInterfaces();
        }

        ThriftPoolProxy thriftPoolProxy = new ThriftPoolProxy();
        thriftPoolProxy.pool = ThriftGenericKeyedObjectPool.getInstance(processRefer);
        thriftPoolProxy.key = new EndPoint(clazz, url);

        @SuppressWarnings("unchecked")
        T action = (T) Proxy.newProxyInstance(clazz.getClassLoader(), interfaces, thriftPoolProxy);
        return action;
    }
}
