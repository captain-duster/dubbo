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

import com.app.shutdown.ApplicationDrainState;
import com.app.shutdown.InFlightWorkRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.stereotype.Component;

@Component
public class DrainingRecordInterceptor<K, V> implements RecordInterceptor<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(DrainingRecordInterceptor.class);

    private final InFlightWorkRegistry inFlightWorkRegistry;

    public DrainingRecordInterceptor(InFlightWorkRegistry inFlightWorkRegistry) {
        this.inFlightWorkRegistry = inFlightWorkRegistry;
    }

    @Override
    public ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        if (ApplicationDrainState.getCurrentState() == ApplicationDrainState.DRAINING) {
            logger.info(
                    "Rejecting Kafka record from topic {} partition {} offset {} because application is draining",
                    record.topic(),
                    record.partition(),
                    record.offset());
            return null;
        }
        inFlightWorkRegistry.register(InFlightWorkRegistry.CATEGORY_KAFKA);
        return record;
    }

    @Override
    public void success(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        inFlightWorkRegistry.deregister(InFlightWorkRegistry.CATEGORY_KAFKA);
    }

    @Override
    public void failure(ConsumerRecord<K, V> record, Exception exception, Consumer<K, V> consumer) {
        inFlightWorkRegistry.deregister(InFlightWorkRegistry.CATEGORY_KAFKA);
    }
}
