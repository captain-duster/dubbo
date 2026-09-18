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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.app.shutdown.config.ShutdownProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InFlightWorkRegistryTest {

    private ShutdownProperties shutdownProperties;
    private InFlightWorkRegistry inFlightWorkRegistry;
    private ExecutorService executorService;

    @BeforeEach
    public void setUp() {
        shutdownProperties = new ShutdownProperties();
        shutdownProperties.setPollIntervalMillis(50);
        inFlightWorkRegistry = new InFlightWorkRegistry(shutdownProperties);
        executorService = Executors.newFixedThreadPool(8);
    }

    @AfterEach
    public void tearDown() {
        executorService.shutdownNow();
    }

    @Test
    public void shouldStartWithZeroInFlightCounts() {
        assertEquals(0, inFlightWorkRegistry.getTotalInFlightCount());
        assertEquals(0, inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_KAFKA));
        assertEquals(0, inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_HTTP));
        assertEquals(0, inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_DB));
    }

    @Test
    public void shouldIncrementAndDecrementCountersPerCategory() {
        inFlightWorkRegistry.register(InFlightWorkRegistry.CATEGORY_HTTP);
        inFlightWorkRegistry.register(InFlightWorkRegistry.CATEGORY_HTTP);
        inFlightWorkRegistry.register(InFlightWorkRegistry.CATEGORY_KAFKA);

        assertEquals(2, inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_HTTP));
        assertEquals(1, inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_KAFKA));
        assertEquals(3, inFlightWorkRegistry.getTotalInFlightCount());

        inFlightWorkRegistry.deregister(InFlightWorkRegistry.CATEGORY_HTTP);
        inFlightWorkRegistry.deregister(InFlightWorkRegistry.CATEGORY_KAFKA);

        assertEquals(1, inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_HTTP));
        assertEquals(0, inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_KAFKA));
        assertEquals(1, inFlightWorkRegistry.getTotalInFlightCount());
    }

    @Test
    public void shouldHandleConcurrentRegisterAndDeregisterCorrectly() throws InterruptedException {
        int threadCount = 8;
        int iterationsPerThread = 500;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executorService.execute(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < iterationsPerThread; j++) {
                        inFlightWorkRegistry.register(InFlightWorkRegistry.CATEGORY_DB);
                        inFlightWorkRegistry.deregister(InFlightWorkRegistry.CATEGORY_DB);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = completionLatch.await(5, TimeUnit.SECONDS);

        assertTrue(completed);
        assertEquals(0, inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_DB));
        assertEquals(0, inFlightWorkRegistry.getTotalInFlightCount());
    }

    @Test
    public void shouldReturnTrueImmediatelyWhenNoInFlightWork() {
        boolean drained = inFlightWorkRegistry.awaitDrain(1000);

        assertTrue(drained);
    }

    @Test
    public void shouldBlockUntilInFlightWorkCompletesThenReturnTrue() throws InterruptedException {
        inFlightWorkRegistry.register(InFlightWorkRegistry.CATEGORY_HTTP);

        executorService.execute(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            inFlightWorkRegistry.deregister(InFlightWorkRegistry.CATEGORY_HTTP);
        });

        long start = System.currentTimeMillis();
        boolean drained = inFlightWorkRegistry.awaitDrain(5000);
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(drained);
        assertTrue(elapsed >= 150);
        assertEquals(0, inFlightWorkRegistry.getTotalInFlightCount());
    }

    @Test
    public void shouldReturnFalseWhenTimeoutElapsesWithInFlightWorkRemaining() {
        inFlightWorkRegistry.register(InFlightWorkRegistry.CATEGORY_KAFKA);

        boolean drained = inFlightWorkRegistry.awaitDrain(300);

        assertFalse(drained);
        assertEquals(1, inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_KAFKA));

        inFlightWorkRegistry.deregister(InFlightWorkRegistry.CATEGORY_KAFKA);
    }
}
