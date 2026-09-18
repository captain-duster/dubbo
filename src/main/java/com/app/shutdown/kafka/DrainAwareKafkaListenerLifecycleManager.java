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
package com.app.shutdown.kafka;

import javax.annotation.PreDestroy;

import com.app.shutdown.InFlightWorkRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
public class DrainAwareKafkaListenerLifecycleManager {

    private static final Logger logger = LoggerFactory.getLogger(DrainAwareKafkaListenerLifecycleManager.class);

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private final InFlightWorkRegistry inFlightWorkRegistry;

    public DrainAwareKafkaListenerLifecycleManager(
            KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry, InFlightWorkRegistry inFlightWorkRegistry) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
        this.inFlightWorkRegistry = inFlightWorkRegistry;
    }

    @PreDestroy
    public void stopConsumingNewRecords() {
        int pendingKafkaWork = inFlightWorkRegistry.getInFlightCount(InFlightWorkRegistry.CATEGORY_KAFKA);
        logger.info(
                "Pausing all Kafka listener containers to stop consuming new records, {} in-flight kafka messages pending",
                pendingKafkaWork);

        for (MessageListenerContainer container : kafkaListenerEndpointRegistry.getListenerContainers()) {
            if (container.isRunning()) {
                container.pause();
                logger.info("Paused Kafka listener container: {}", container.getListenerId());
            }
        }

        for (MessageListenerContainer container : kafkaListenerEndpointRegistry.getListenerContainers()) {
            if (container.isRunning()) {
                container.stop();
                logger.info("Stopped Kafka listener container: {}", container.getListenerId());
            }
        }
    }
}
