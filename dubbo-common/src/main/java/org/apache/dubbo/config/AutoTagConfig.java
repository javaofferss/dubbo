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
package org.apache.dubbo.config;

import org.apache.dubbo.config.annotation.AutoTag;
import org.apache.dubbo.config.support.Parameter;

import java.io.Serializable;

/**
 * Configuration backing {@link AutoTag}: automatic tag routing driven by a method argument.
 * <p>
 * Held by {@code ReferenceConfig} as a nested config. It is intentionally <b>not</b> annotated
 * {@code @Nested}, so {@code AbstractConfig.appendParameters} skips it; the three values are
 * written to URL parameters explicitly in {@code ReferenceConfig.appendConfig()}.
 *
 * @see AutoTag
 * @since 3.3.7
 */
public class AutoTagConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Whether to enable automatic tag injection from a method argument.
     */
    private boolean enabled = false;

    /**
     * Index of the argument used as the tag source.
     */
    private int index = 0;

    /**
     * Whether to force tag routing when no tagged provider matches.
     */
    private boolean force = true;

    public AutoTagConfig() {}

    public AutoTagConfig(AutoTag autoTag) {
        if (autoTag != null) {
            this.enabled = autoTag.enabled();
            this.index = autoTag.index();
            this.force = autoTag.force();
        }
    }

    @Parameter(excluded = true)
    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Parameter(excluded = true)
    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Parameter(excluded = true)
    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }
}
