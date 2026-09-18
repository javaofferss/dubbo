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

import java.util.function.BiFunction;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

/**
 * 线程池
 * @param <T>
 */
public class ThriftGenericKeyedObjectPool<T> extends GenericKeyedObjectPool<ThriftGenericKeyedObjectPool.EndPoint, T> {

    public ThriftGenericKeyedObjectPool(final KeyedPooledObjectFactory factory) {
        super(factory);
    }

    public static class ThriftKeyedPooledObjectFactory<T> extends BaseKeyedPooledObjectFactory<EndPoint<T>, T> {

        BiFunction<Class<T>, URL, T> processRefer;

        @Override
        public T create(EndPoint key) throws Exception {
            return processRefer.apply((Class<T>) key.tClass, key.url);
        }

        @Override
        public PooledObject<T> wrap(Object value) {
            return new DefaultPooledObject(value);
        }
    }

    public static class EndPoint<T> {
        Class<T> tClass;
        URL url;

        public EndPoint(Class<T> tClass, URL url) {
            this.tClass = tClass;
            this.url = url;
        }

        @Override
        public boolean equals(Object obj) {
            return toString().equals(obj.toString());
        }

        @Override
        public int hashCode() {
            return toString().hashCode();
        }

        @Override
        public String toString() {
            return url.toString();
        }
    }

    public static ThriftGenericKeyedObjectPool getInstance(BiFunction<Class, URL, Object> processRefer) {
        ThriftKeyedPooledObjectFactory factory = new ThriftKeyedPooledObjectFactory();
        factory.processRefer = processRefer;
        return new ThriftGenericKeyedObjectPool(factory);
    }
}
