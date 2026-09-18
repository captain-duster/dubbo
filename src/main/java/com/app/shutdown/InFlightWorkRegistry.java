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
package com.app.shutdown;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.app.shutdown.config.ShutdownProperties;
import org.springframework.stereotype.Component;

@Component
public class InFlightWorkRegistry {

    public static final String CATEGORY_KAFKA = "kafka";
    public static final String CATEGORY_HTTP = "http";
    public static final String CATEGORY_DB = "db";

    private final ConcurrentHashMap<String, AtomicInteger> countersByCategory = new ConcurrentHashMap<>();
    private final AtomicInteger totalCount = new AtomicInteger(0);
    private final ShutdownProperties shutdownProperties;

    public InFlightWorkRegistry(ShutdownProperties shutdownProperties) {
        this.shutdownProperties = shutdownProperties;
    }

    public void register(String category) {
        countersByCategory
                .computeIfAbsent(category, key -> new AtomicInteger(0))
                .incrementAndGet();
        totalCount.incrementAndGet();
    }

    public void deregister(String category) {
        AtomicInteger counter = countersByCategory.get(category);
        if (counter != null) {
            counter.decrementAndGet();
        }
        totalCount.decrementAndGet();
    }

    public int getInFlightCount(String category) {
        AtomicInteger counter = countersByCategory.get(category);
        return counter == null ? 0 : counter.get();
    }

    public int getTotalInFlightCount() {
        return totalCount.get();
    }

    public boolean awaitDrain(long timeoutMillis) {
        long pollIntervalMillis = shutdownProperties.getPollIntervalMillis();
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (totalCount.get() <= 0) {
                return true;
            }
            long remaining = deadline - System.currentTimeMillis();
            long sleepMillis = Math.min(pollIntervalMillis, remaining);
            if (sleepMillis <= 0) {
                break;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return totalCount.get() <= 0;
            }
        }
        return totalCount.get() <= 0;
    }
}
