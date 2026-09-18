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
package com.app.shutdown.health;

import com.app.shutdown.ApplicationDrainState;
import com.app.shutdown.InFlightWorkRegistry;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.stereotype.Component;

@Component
public class DrainHealthIndicator implements HealthIndicator {

    private final InFlightWorkRegistry inFlightWorkRegistry;

    public DrainHealthIndicator(InFlightWorkRegistry inFlightWorkRegistry) {
        this.inFlightWorkRegistry = inFlightWorkRegistry;
    }

    @Override
    public Health health() {
        ApplicationDrainState currentState = ApplicationDrainState.getCurrentState();

        if (currentState == ApplicationDrainState.DRAINING) {
            return Health.status(Status.OUT_OF_SERVICE)
                    .withDetail("state", currentState)
                    .withDetail(
                            "kafkaInFlight", inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_KAFKA))
                    .withDetail(
                            "httpInFlight", inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_HTTP))
                    .withDetail("dbInFlight", inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_DB))
                    .build();
        }

        if (currentState == ApplicationDrainState.STOPPED) {
            return Health.status(Status.DOWN).withDetail("state", currentState).build();
        }

        return Health.up().withDetail("state", currentState).build();
    }
}
