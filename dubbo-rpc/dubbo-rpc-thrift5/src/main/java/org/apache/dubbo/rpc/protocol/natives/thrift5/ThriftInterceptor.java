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

import org.apache.thrift.transport.TTransport;
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

    public void before(ThriftGenericKeyedObjectPool.EndPoint key , String methodName, Object... args) {

    }

    public void success(ThriftGenericKeyedObjectPool.EndPoint key, String methodName, Object response, Object... args) {

    }

    public void error(ThriftGenericKeyedObjectPool.EndPoint key, String methodName, Throwable throwable, Object... args) {

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
                    Throwable cause = throwable.getCause();
                    if (throwable instanceof TTransportException) {
                        int type = ((TTransportException) throwable).getType();
                        switch (type) {
                            case TTransportException.END_OF_FILE: // 连接关闭了. 当然还有其他的。这里暂时不处理了
                                pool.invalidateObject(key, client); // 直接无效
                                closeClient(client);
                                return;
                            default:
                                break;
                        }
                    }else if(cause instanceof TTransportException){
                        int type = ((TTransportException) cause).getType();
                        switch (type) {
                            case TTransportException.END_OF_FILE: // 连接关闭了. 当然还有其他的。这里暂时不处理了
                                pool.invalidateObject(key, client); // 直接无效
                                closeClient(client);
                                return;
                            default:
                                break;
                        }
                    }
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
            pool.returnObject(key, client);
        }
    }

    private void closeClient(Object o){
        try {
            if(o instanceof TTransport){
                TTransport transport = (TTransport) o;
                transport.close();
            }
        }catch (Exception e){
            //ignore
        }
    }

    public void over(Object client, String methodName) {

    }
}
