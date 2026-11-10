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
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;

/**
 * 线程池
 * @param <T>
 */
public class ThriftGenericKeyedObjectPool<T> extends GenericKeyedObjectPool<ThriftGenericKeyedObjectPool.EndPoint, T> {

    public ThriftGenericKeyedObjectPool(final KeyedPooledObjectFactory factory) {
        super(factory);
    }

    public ThriftGenericKeyedObjectPool(final KeyedPooledObjectFactory factory, final GenericKeyedObjectPoolConfig config) {
        super(factory, config);
    }

    public static class ThriftKeyedPooledObjectFactory<T> extends BaseKeyedPooledObjectFactory<EndPoint<T>, T> {

        BiFunction<Class<T>, URL, T> processRefer;

        @Override
        public T create(EndPoint key) throws Exception {
            T client = processRefer.apply((Class<T>) key.tClass, key.url);
            return client;
        }

        @Override
        public PooledObject<T> wrap(Object value) {
            return new DefaultPooledObject(value);
        }

        @Override
        public boolean validateObject(EndPoint<T> key, PooledObject<T> p) {
            TServiceClient client = (TServiceClient) p.getObject();
            return client.getInputProtocol().getTransport().isOpen() &&
                    client.getOutputProtocol().getTransport().isOpen();
        }

        @Override
        public void destroyObject(EndPoint<T> key, PooledObject<T> p) throws Exception {
            T client = p.getObject();
            closeClient((TServiceClient) client);
        }
    }

    static void closeClient(TServiceClient client) {
        if (client != null) {
            client.getInputProtocol().getTransport().close();
            client.getOutputProtocol().getTransport().close();
        }
    }

    public static class EndPoint<T> {
        Class<T> tClass;
        URL url;
        String host;
        int port;
        String key;

        public EndPoint(Class<T> tClass, URL url) {
            this.tClass = tClass;
            this.url = url;
            this.host = url.getHost();
            this.port = url.getPort();
            this.key = host+"_"+port;
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof EndPoint){
                return ((EndPoint<?>) obj).key.equals(this.key);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return this.key.hashCode();
        }

        @Override
        public String toString() {
            return url.toString();
        }

        public URL getUrl() {
            return url;
        }
    }

    public static ThriftGenericKeyedObjectPool getInstance(BiFunction<Class, URL, Object> processRefer, URL url) {
        ThriftKeyedPooledObjectFactory factory = new ThriftKeyedPooledObjectFactory();
        factory.processRefer = processRefer;

        // 线上默认连接池配置：以稳定、低抖动为目标，吞吐优先，兼顾空闲回收。
        // URL 参数可覆盖任意默认值：pool.maxTotal / pool.maxTotalPerKey / pool.maxIdlePerKey
        // / pool.minIdlePerKey / pool.maxWaitMillis / pool.timeBetweenEvictionRunsMillis
        // / pool.minEvictableIdleTimeMillis / pool.numTestsPerEvictionRun / pool.blockWhenExhausted
        // / pool.testOnBorrow / pool.testOnReturn / pool.testWhileIdle / pool.fairness
        GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
        config.setMaxTotal(url.getParameter("pool.maxActiveConnection", 200));
        config.setMaxTotalPerKey(url.getParameter("pool.maxActiveConnection", 200));
        //下面这两个在测qps时所调整后的值64-16。120并发，2.9w/qps
        config.setMaxIdlePerKey(url.getParameter("pool.maxIdlePerKey", 64));
        config.setMinIdlePerKey(url.getParameter("pool.minIdlePerKey", 16));
        // 借出时阻塞等待，最多等待 3s，避免突发流量下直接抛异常
        config.setBlockWhenExhausted(url.getParameter("pool.blockWhenExhausted", true));
        config.setMaxWaitMillis(url.getParameter("pool.maxWaitMillis", 3000L));
        // 关闭 borrow/return 时的连通性校验（影响吞吐），仅依赖空闲驱逐线程做存活检测
        config.setTestOnBorrow(url.getParameter("pool.testOnBorrow", true));
        config.setTestOnReturn(url.getParameter("pool.testOnReturn", false));
        config.setTestWhileIdle(url.getParameter("pool.testWhileIdle", true));
        // 空闲对象驱逐线程：每 30s 跑一次，每次检测 3 个连接，空闲超过 10min 回收
        config.setTimeBetweenEvictionRunsMillis(url.getParameter("pool.timeBetweenEvictionRunsMillis", 30_000L));
        config.setNumTestsPerEvictionRun(url.getParameter("pool.numTestsPerEvictionRun", 3));
        config.setMinEvictableIdleTimeMillis(url.getParameter("pool.minEvictableIdleTimeMillis", 600_000L));
        config.setFairness(url.getParameter("pool.fairness", false));

        //每一个key只有一个池. 每个key隔离. 所以maxTotal 和 maxTotalPerKey 设置的是一样的.
        ThriftGenericKeyedObjectPool pool = new ThriftGenericKeyedObjectPool(factory, config);

        return pool;
    }
}
