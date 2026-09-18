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
package com.app.shutdown.actuator;

import java.util.LinkedHashMap;
import java.util.Map;

import com.app.shutdown.ApplicationDrainState;
import com.app.shutdown.InFlightWorkRegistry;
import com.app.shutdown.config.ShutdownProperties;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

@Component
@Endpoint(id = "drainStatus")
public class DrainStatusEndpoint {

    private final InFlightWorkRegistry inFlightWorkRegistry;
    private final ShutdownProperties shutdownProperties;

    public DrainStatusEndpoint(InFlightWorkRegistry inFlightWorkRegistry, ShutdownProperties shutdownProperties) {
        this.inFlightWorkRegistry = inFlightWorkRegistry;
        this.shutdownProperties = shutdownProperties;
    }

    @ReadOperation
    public Map<String, Object> drainStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("state", ApplicationDrainState.getCurrentState());
        status.put("kafkaInFlight", inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_KAFKA));
        status.put("httpInFlight", inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_HTTP));
        status.put("dbInFlight", inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_DB));
        status.put("totalInFlight", inFlightWorkRegistry.getTotalInFlightCount());
        status.put("timeoutSeconds", shutdownProperties.getTimeoutSeconds());
        return status;
    }
}
