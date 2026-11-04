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
package org.apache.dubbo.config.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Configuration for automatic tag routing driven by a method argument.
 * <p>
 * When {@link #enabled()} is true, the argument at {@link #index()} (default 0, the first
 * argument) is converted to a string and set as the request tag ({@code dubbo.tag}) on the
 * invocation before cluster routing, so {@code TagStateRouter} can route to providers tagged
 * with that value. Off by default.
 *
 * @see DubboReference#autoTag()
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
public @interface AutoTag {

    /**
     * Whether to enable automatic tag injection from a method argument. Off by default.
     */
    boolean enabled() default false;

    /**
     * Index of the argument to use as the tag source when enabled. Default 0 (first argument).
     */
    int index() default 0;

    /**
     * Whether to force tag routing (set {@code force.tag=true}) when no tagged provider matches.
     * <p>
     * Default true: fail fast instead of silently falling back to untagged providers, which would
     * cause cross-tag routing (e.g. cross-tenant). Set false to follow TagStateRouter's default
     * fallback behavior (route to untagged providers when no match).
     */
    boolean force() default true;
}
