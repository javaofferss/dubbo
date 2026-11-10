package org.apache.dubbo.rpc;

/**
 * create by cmj on 00:31
 */
public class RpcIface<T> {

    private T reference;

    private RpcIface(T reference) {
        this.reference = reference;
    }

    public RpcIface<T> setTimeOut(long timeOut) {
        //
        return this;
    }

    public RpcIface<T> setDubboTag(String tag){
        RpcContext.getClientAttachment().setAttachment("dubbo.tag", tag);
        return this;
    }
    public T rpcCall(){
        return reference;
    }
}
