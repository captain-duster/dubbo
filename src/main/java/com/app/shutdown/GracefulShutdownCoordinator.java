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

import java.util.concurrent.TimeUnit;

import com.app.shutdown.config.ShutdownProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

@Component
public class GracefulShutdownCoordinator implements SmartLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(GracefulShutdownCoordinator.class);

    private static final int SHUTDOWN_PHASE = Integer.MAX_VALUE;

    private final InFlightWorkRegistry inFlightWorkRegistry;
    private final ShutdownProperties shutdownProperties;

    private volatile boolean running = false;

    public GracefulShutdownCoordinator(
            InFlightWorkRegistry inFlightWorkRegistry, ShutdownProperties shutdownProperties) {
        this.inFlightWorkRegistry = inFlightWorkRegistry;
        this.shutdownProperties = shutdownProperties;
    }

    @Override
    public void start() {
        ApplicationDrainState.setCurrentState(ApplicationDrainState.RUNNING);
        running = true;
    }

    @Override
    public void stop() {
        stop(() -> {});
    }

    @Override
    public void stop(Runnable callback) {
        ApplicationDrainState.setCurrentState(ApplicationDrainState.DRAINING);
        logger.info("Graceful shutdown initiated, draining in-flight work");

        long timeoutMillis = TimeUnit.SECONDS.toMillis(shutdownProperties.getTimeoutSeconds());
        boolean drained = inFlightWorkRegistry.awaitDrain(timeoutMillis);

        if (!drained) {
            int kafkaCount = inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_KAFKA);
            int httpCount = inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_HTTP);
            int dbCount = inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_DB);
            logger.warn(
                    "Drain timeout of {} seconds elapsed with in-flight work remaining: kafka={}, http={}, db={}",
                    shutdownProperties.getTimeoutSeconds(),
                    kafkaCount,
                    httpCount,
                    dbCount);
        } else {
            logger.info("Drain completed successfully, no in-flight work remaining");
        }

        ApplicationDrainState.setCurrentState(ApplicationDrainState.STOPPED);
        running = false;
        callback.run();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return SHUTDOWN_PHASE;
    }
}
