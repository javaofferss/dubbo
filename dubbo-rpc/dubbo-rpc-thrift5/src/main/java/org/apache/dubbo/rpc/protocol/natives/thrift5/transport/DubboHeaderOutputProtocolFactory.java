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
package org.apache.dubbo.rpc.protocol.natives.thrift5.transport;

import org.apache.dubbo.rpc.protocol.natives.thrift5.codec.DubboHeader;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;

/**
 * 提供端响应方向 hook（见 protocol-header-design.md §3.3）。
 *
 * <p>{@link TNonblockingServer.FrameBuffer#invoke()} 调用
 * {@code outputProtocolFactory_.getProtocol(getOutputTransport())} 构造响应 protocol；其中
 * {@code getOutputTransport()} 返回 {@code TFramedTransport(TIOStreamTransport(response_))}。
 *
 * <p>本 factory 返回 {@link TBinaryProtocol} 子类，重写 {@link TBinaryProtocol#writeMessageBegin}：
 * 在写 thrift 响应消息之前先写 Dubbo 响应头。此时业务方法已返回、result attachment 已就绪，
 * 故<b>无需缓冲、无需 flush 信号</b>——头字节直接随 thrift 消息进入 {@code response_}，
 * 最终由 {@code TFramedTransport.flush()} 补 4 字节 frame 长度。
 *
 * <p>阶段一写空头（{@link DubboHeader#writeEmpty}）即与消费端 {@link DubboHeaderClientTransport} 对称；
 * 阶段二在此改为 {@link DubboHeader#write} 填 {@code RpcContext.getServerResponseContext()} 附件即可平滑升级。
 */
public class DubboHeaderOutputProtocolFactory implements TProtocolFactory {

    private static final long serialVersionUID = 0L;

    public DubboHeaderOutputProtocolFactory() {}

    @Override
    public TProtocol getProtocol(final TTransport trans) {
        // thrift5 wire 固定使用 TBinaryProtocol 默认参数（strictRead=false, strictWrite=true），
        // 与 Thrift5Protocol 中写死的 TBinaryProtocol.Factory() 一致。
        return new TBinaryProtocol(trans) {
            @Override
            public void writeMessageBegin(TMessage message) throws org.apache.thrift.TException {
                // 阶段一：写空头（headerLen=0），与消费端读响应头路径对称
                DubboHeader.writeEmpty(trans);
                super.writeMessageBegin(message);
            }
        };
    }
}
