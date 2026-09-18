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
package com.app.shutdown.web;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import com.app.shutdown.ApplicationDrainState;
import com.app.shutdown.InFlightWorkRegistry;
import com.app.shutdown.config.ShutdownProperties;
import org.springframework.stereotype.Component;

@Component
public class DrainAwareRequestFilter implements Filter {

    private final InFlightWorkRegistry inFlightWorkRegistry;
    private final ShutdownProperties shutdownProperties;

    public DrainAwareRequestFilter(InFlightWorkRegistry inFlightWorkRegistry, ShutdownProperties shutdownProperties) {
        this.inFlightWorkRegistry = inFlightWorkRegistry;
        this.shutdownProperties = shutdownProperties;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (ApplicationDrainState.getCurrentState() == ApplicationDrainState.DRAINING) {
            if (response instanceof HttpServletResponse) {
                HttpServletResponse httpServletResponse = (HttpServletResponse) response;
                httpServletResponse.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                httpServletResponse.setHeader("Retry-After", String.valueOf(shutdownProperties.getTimeoutSeconds()));
            }
            return;
        }

        inFlightWorkRegistry.register(InFlightWorkRegistry.CATEGORY_HTTP);
        try {
            chain.doFilter(request, response);
        } finally {
            inFlightWorkRegistry.deregister(InFlightWorkRegistry.CATEGORY_HTTP);
        }
    }
}
