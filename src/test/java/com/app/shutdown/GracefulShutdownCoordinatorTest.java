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
import java.util.concurrent.atomic.AtomicBoolean;

import com.app.shutdown.config.ShutdownProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GracefulShutdownCoordinatorTest {

    private ShutdownProperties shutdownProperties;
    private InFlightWorkRegistry inFlightWorkRegistry;
    private GracefulShutdownCoordinator gracefulShutdownCoordinator;
    private ExecutorService executorService;

    @BeforeEach
    public void setUp() {
        shutdownProperties = new ShutdownProperties();
        shutdownProperties.setPollIntervalMillis(50);
        inFlightWorkRegistry = new InFlightWorkRegistry(shutdownProperties);
        gracefulShutdownCoordinator = new GracefulShutdownCoordinator(inFlightWorkRegistry, shutdownProperties);
        executorService = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    public void tearDown() {
        executorService.shutdownNow();
    }

    @Test
    public void shouldWaitForInFlightWorkToCompleteBeforeInvokingCallback() throws InterruptedException {
        shutdownProperties.setTimeoutSeconds(5);
        gracefulShutdownCoordinator.start();
        inFlightWorkRegistry.register(InFlightWorkRegistry.CATEGORY_HTTP);

        AtomicBoolean callbackInvoked = new AtomicBoolean(false);
        CountDownLatch callbackLatch = new CountDownLatch(1);

        executorService.execute(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            inFlightWorkRegistry.deregister(InFlightWorkRegistry.CATEGORY_HTTP);
        });

        gracefulShutdownCoordinator.stop(() -> {
            callbackInvoked.set(true);
            callbackLatch.countDown();
        });

        boolean callbackCompleted = callbackLatch.await(6, TimeUnit.SECONDS);

        assertTrue(callbackCompleted);
        assertTrue(callbackInvoked.get());
        assertEquals(0, inFlightWorkRegistry.getTotalInFlightCount());
        assertEquals(ApplicationDrainState.STOPPED, ApplicationDrainState.getCurrentState());
        assertTrue(!gracefulShutdownCoordinator.isRunning());
    }

    @Test
    public void shouldForceShutdownAfterTimeoutExpiryWithPendingWork() throws InterruptedException {
        shutdownProperties.setTimeoutSeconds(1);
        gracefulShutdownCoordinator.start();
        inFlightWorkRegistry.register(InFlightWorkRegistry.CATEGORY_KAFKA);

        AtomicBoolean callbackInvoked = new AtomicBoolean(false);
        CountDownLatch callbackLatch = new CountDownLatch(1);

        gracefulShutdownCoordinator.stop(() -> {
            callbackInvoked.set(true);
            callbackLatch.countDown();
        });

        boolean callbackCompleted = callbackLatch.await(3, TimeUnit.SECONDS);

        assertTrue(callbackCompleted);
        assertTrue(callbackInvoked.get());
        assertEquals(1, inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_KAFKA));
        assertEquals(ApplicationDrainState.STOPPED, ApplicationDrainState.getCurrentState());

        inFlightWorkRegistry.deregister(InFlightWorkRegistry.CATEGORY_KAFKA);
    }

    @Test
    public void shouldTransitionToDrainingStateWhenStopIsInvoked() {
        shutdownProperties.setTimeoutSeconds(1);
        gracefulShutdownCoordinator.start();
        assertEquals(ApplicationDrainState.RUNNING, ApplicationDrainState.getCurrentState());

        gracefulShutdownCoordinator.stop(() -> {});

        assertEquals(ApplicationDrainState.STOPPED, ApplicationDrainState.getCurrentState());
    }

    @Test
    public void shouldReportVeryHighPhaseSoItRunsLast() {
        assertEquals(Integer.MAX_VALUE, gracefulShutdownCoordinator.getPhase());
    }

    @Test
    public void shouldBeRunningAfterStartAndAutoStartupTrue() {
        assertTrue(gracefulShutdownCoordinator.isAutoStartup());
        gracefulShutdownCoordinator.start();
        assertTrue(gracefulShutdownCoordinator.isRunning());
    }
}
