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
package com.app.shutdown.db;

import com.app.shutdown.InFlightWorkRegistry;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class DrainAwareRepositoryAspect {

    private final InFlightWorkRegistry inFlightWorkRegistry;

    public DrainAwareRepositoryAspect(InFlightWorkRegistry inFlightWorkRegistry) {
        this.inFlightWorkRegistry = inFlightWorkRegistry;
    }

    @Pointcut("execution(* org.springframework.data.repository.Repository+.*(..))")
    public void repositoryMethod() {}

    @Pointcut("@annotation(org.springframework.transaction.annotation.Transactional)")
    public void transactionalMethod() {}

    @Around("repositoryMethod() || transactionalMethod()")
    public Object trackInFlightDatabaseWork(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        inFlightWorkRegistry.register(InFlightWorkRegistry.CATEGORY_DB);
        try {
            return proceedingJoinPoint.proceed();
        } finally {
            inFlightWorkRegistry.deregister(InFlightWorkRegistry.CATEGORY_DB);
        }
    }
}
