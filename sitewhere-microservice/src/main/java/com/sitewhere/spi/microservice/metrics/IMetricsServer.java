/**
 * Copyright © 2014-2021 The SiteWhere Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sitewhere.spi.microservice.metrics;

import com.sitewhere.spi.microservice.lifecycle.ILifecycleComponent;

/**
 * Provides Prometheus metrics over HTTP.
 */
public interface IMetricsServer extends ILifecycleComponent {

    /**
     * Register an additional endpoint to be served by the HTTP server. If the
     * server is not currently running, the endpoint will be attached when the
     * server starts.
     * 
     * @param endpoint
     */
    void registerEndpoint(IMetricsHttpEndpoint endpoint);

    /**
     * Unregister a previously added endpoint.
     * 
     * @param endpoint
     */
    void unregisterEndpoint(IMetricsHttpEndpoint endpoint);
}
