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
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.filter.ClusterFilter;

import static org.apache.dubbo.common.constants.CommonConstants.AUTO_TAG_ARG_INDEX_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.AUTO_TAG_FORCE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.AUTO_TAG_FROM_ARG_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;
import static org.apache.dubbo.common.constants.CommonConstants.TAG_KEY;
import static org.apache.dubbo.rpc.Constants.FORCE_USE_TAG;

/**
 * Automatically injects the request tag ({@code dubbo.tag}) from a method argument before cluster
 * routing, so {@code TagStateRouter} can route to providers tagged with that value.
 * <p>
 * Runs as a {@link ClusterFilter}, which executes <b>before</b> {@code ClusterInvoker} (routing +
 * load balancing) — this is the only SPI point early enough for the injected tag to be visible to
 * {@code TagStateRouter.doRoute}. A regular consumer {@code Filter} runs after routing and would
 * be too late.
 * <p>
 * Activated only on references whose consumer URL carries {@code auto.tag.from.arg=true} (set from
 * {@link org.apache.dubbo.config.annotation.AutoTag#enabled()}), so references that did not opt in
 * pay no overhead — the filter is not even in their chain.
 * <p>
 * Rules:
 * <ul>
 *   <li>If the caller already set {@code dubbo.tag} explicitly, it is respected (not overridden).</li>
 *   <li>If the source argument is null / index out of range / empty string, no tag is injected and
 *       the call falls through to normal routing.</li>
 *   <li>{@code force.tag} is set from {@code auto.tag.force} (default true): fail fast when no
 *       tagged provider matches, instead of silently falling back to untagged providers.</li>
 * </ul>
 *
 * @see org.apache.dubbo.config.annotation.AutoTag
 * @since 3.3.7
 */
@Activate(group = CONSUMER, value = AUTO_TAG_FROM_ARG_KEY, order = Integer.MIN_VALUE + 1000)
public class AutoTagClusterFilter implements ClusterFilter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation inv) throws RpcException {
        URL url = invoker.getUrl();

        // @Activate(key=...) guarantees only opted-in references attach this filter; double-check.
        if (!url.getParameter(AUTO_TAG_FROM_ARG_KEY, false)) {
            return invoker.invoke(inv);
        }

        // Rule 1: respect an explicitly set tag — do not override a manual per-call tag.
        if (StringUtils.isNotEmpty(inv.getAttachment(TAG_KEY))) {
            return invoker.invoke(inv);
        }

        // Resolve the source argument.
        int idx = url.getParameter(AUTO_TAG_ARG_INDEX_KEY, 0);
        Object[] args = inv.getArguments();
        if (args == null || idx < 0 || idx >= args.length) {
            return invoker.invoke(inv); // out of range → skip
        }
        Object src = args[idx];
        if (src == null) {
            return invoker.invoke(inv); // null → skip
        }
        String tag = String.valueOf(src);
        if (StringUtils.isEmpty(tag)) {
            return invoker.invoke(inv);
        }

        // Inject the tag; TagStateRouter reads invocation.getAttachment(TAG_KEY) afterwards.
        if (inv instanceof RpcInvocation) {
            RpcInvocation rpcInv = (RpcInvocation) inv;
            rpcInv.setAttachment(TAG_KEY, tag);
            // force: default true (read from URL). Fail fast on no match to avoid silent cross-tag routing.
            boolean force = url.getParameter(AUTO_TAG_FORCE_KEY, true);
            rpcInv.setAttachment(FORCE_USE_TAG, String.valueOf(force));
        }
        return invoker.invoke(inv);
    }
}
