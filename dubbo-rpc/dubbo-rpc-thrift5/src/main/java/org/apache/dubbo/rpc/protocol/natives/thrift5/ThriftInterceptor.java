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

import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * thrift 拦截器.
 */
public class ThriftInterceptor {

    private static final Logger log = LoggerFactory.getLogger(ThriftInterceptor.class);

    public Object borrowClient(ThriftGenericKeyedObjectPool pool, ThriftGenericKeyedObjectPool.EndPoint key)
            throws Exception {
        return pool.borrowObject(key);
    }

    public void before(Object client, String methodName, Object... args) {
        log.info("methodName {}", methodName);
    }

    public void success(Object client, String methodName, Object response) {
        log.info("methodName {}", methodName);
    }

    public void error(Object client, String methodName, Throwable throwable) {
        log.info("methodName {}", methodName);
    }

    public void recycleClient(
            ThriftGenericKeyedObjectPool pool,
            ThriftGenericKeyedObjectPool.EndPoint key,
            Object client,
            Throwable throwable) {
        if (client != null) {
            // 再判断是否有异常。这里的逻辑就不具体写了。主要可以判断一下socket是否关闭了。如果关闭了就不应该放回了，应该直接销毁异常的客户端
            if (throwable != null) {
                try {
                    if (throwable instanceof TTransportException) {
                        int type = ((TTransportException) throwable).getType();
                        switch (type) {
                            case TTransportException.END_OF_FILE: // 连接关闭了. 当然还有其他的。这里暂时不处理了
                                pool.invalidateObject(key, client); // 直接无效
                                return;
                            default:
                                break;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            pool.returnObject(key, client);
        }
    }

    public void over(Object client, String methodName) {
        log.info("methodName {}", methodName);
    }
}
