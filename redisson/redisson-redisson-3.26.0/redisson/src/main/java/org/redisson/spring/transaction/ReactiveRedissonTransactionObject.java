/**
 * Copyright (c) 2013-2024 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.spring.transaction;

import org.springframework.transaction.support.SmartTransactionObject;

/**
 * @author Nikita Koksharov
 */
public class ReactiveRedissonTransactionObject implements SmartTransactionObject {

    private ReactiveRedissonResourceHolder resourceHolder;

    public ReactiveRedissonResourceHolder getResourceHolder() {
        return resourceHolder;
    }

    public void setResourceHolder(ReactiveRedissonResourceHolder resourceHolder) {
        this.resourceHolder = resourceHolder;
    }

    @Override
    public boolean isRollbackOnly() {
        if (resourceHolder != null) {
            return resourceHolder.isRollbackOnly();
        }
        return false;
    }

    @Override
    public void flush() {
        // skip
    }
}
