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
package com.sitewhere.microservice.metrics;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.sitewhere.microservice.configuration.model.instance.infrastructure.MetricsConfiguration;
import com.sitewhere.microservice.lifecycle.LifecycleComponent;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.microservice.lifecycle.ILifecycleProgressMonitor;
import com.sitewhere.spi.microservice.metrics.IMetricsHttpEndpoint;
import com.sitewhere.spi.microservice.metrics.IMetricsServer;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

/**
 * Provides Prometheus metrics over HTTP. Also allows additional endpoints
 * (registered dynamically by microservices) to be served on the same port.
 */
public class MetricsServer extends LifecycleComponent implements IMetricsServer {

    /** Path under which metrics are exported */
    public static final String METRICS_PATH = "/metrics";

    /** Path under which health check is provided */
    public static final String HEALTH_PATH = "/-/healthy";

    /** Root path */
    public static final String ROOT_PATH = "/";

    /** Number of threads used to service HTTP requests */
    private static final int HTTP_THREAD_COUNT = 16;

    /** HTTP server */
    private HttpServer httpServer;

    /** HTTP request thread pool */
    private ExecutorService httpExecutor;

    /** Endpoints registered by microservice code */
    private Map<String, IMetricsHttpEndpoint> endpoints = new HashMap<>();

    /*
     * @see
     * com.sitewhere.server.lifecycle.LifecycleComponent#start(com.sitewhere.spi.
     * server.lifecycle.ILifecycleProgressMonitor)
     */
    @Override
    public void start(ILifecycleProgressMonitor monitor) throws SiteWhereException {
	MetricsConfiguration metrics = getMicroservice().getInstanceConfiguration().getInfrastructure().getMetrics();
	stopServer();
	try {
	    if (metrics.isEnabled()) {
		this.httpServer = HttpServer.create(new InetSocketAddress(metrics.getHttpPort()), 0);
		this.httpServer.createContext(METRICS_PATH, new MetricsHandler());
		this.httpServer.createContext(HEALTH_PATH, new HealthHandler());
		this.httpServer.createContext(ROOT_PATH, new MetricsHandler());
		this.httpExecutor = Executors.newFixedThreadPool(HTTP_THREAD_COUNT, new ThreadFactory() {

		    private AtomicInteger counter = new AtomicInteger();

		    @Override
		    public Thread newThread(Runnable runnable) {
			Thread thread = new Thread(runnable, "metrics-http-" + counter.incrementAndGet());
			thread.setDaemon(true);
			return thread;
		    }
		});
		this.httpServer.setExecutor(getHttpExecutor());
		this.httpServer.start();

		// Attach endpoints registered before the server started.
		synchronized (getEndpoints()) {
		    for (IMetricsHttpEndpoint endpoint : getEndpoints().values()) {
			getHttpServer().createContext(endpoint.getPath(), endpoint.createHandler());
		    }
		}

		getLogger().info(
			String.format("Microservice metrics available via HTTP on port %s.", metrics.getHttpPort()));
	    } else {
		getLogger().info("Metrics HTTP server is configured as disabled.");
	    }
	} catch (IOException e) {
	    throw new SiteWhereException("Unable to initialize metrics HTTP server.", e);
	}
    }

    /*
     * @see
     * com.sitewhere.server.lifecycle.LifecycleComponent#stop(com.sitewhere.spi.
     * server.lifecycle.ILifecycleProgressMonitor)
     */
    @Override
    public void stop(ILifecycleProgressMonitor monitor) throws SiteWhereException {
	stopServer();
    }

    /**
     * Stop HTTP server if running.
     */
    protected void stopServer() {
	if (getHttpServer() != null) {
	    getHttpServer().stop(0);
	    this.httpServer = null;
	}
	if (getHttpExecutor() != null) {
	    getHttpExecutor().shutdownNow();
	    this.httpExecutor = null;
	}
    }

    /*
     * @see
     * com.sitewhere.spi.microservice.metrics.IMetricsServer#registerEndpoint(
     * com.sitewhere.spi.microservice.metrics.IMetricsHttpEndpoint)
     */
    @Override
    public void registerEndpoint(IMetricsHttpEndpoint endpoint) {
	synchronized (getEndpoints()) {
	    getEndpoints().put(endpoint.getPath(), endpoint);
	    if (getHttpServer() != null) {
		getHttpServer().createContext(endpoint.getPath(), endpoint.createHandler());
	    }
	}
	getLogger().info(String.format("Registered HTTP endpoint '%s' on metrics server.", endpoint.getPath()));
    }

    /*
     * @see
     * com.sitewhere.spi.microservice.metrics.IMetricsServer#unregisterEndpoint(
     * com.sitewhere.spi.microservice.metrics.IMetricsHttpEndpoint)
     */
    @Override
    public void unregisterEndpoint(IMetricsHttpEndpoint endpoint) {
	synchronized (getEndpoints()) {
	    getEndpoints().remove(endpoint.getPath());
	    if (getHttpServer() != null) {
		getHttpServer().removeContext(endpoint.getPath());
	    }
	}
	getLogger().info(String.format("Unregistered HTTP endpoint '%s' from metrics server.", endpoint.getPath()));
    }

    /**
     * Handler that exports Prometheus metrics for the default registry.
     */
    protected class MetricsHandler implements HttpHandler {

	@Override
	public void handle(HttpExchange exchange) throws IOException {
	    try {
		exchange.getResponseHeaders().set("Content-Type", TextFormat.CONTENT_TYPE_004);
		exchange.sendResponseHeaders(200, 0);
		Writer writer = new OutputStreamWriter(exchange.getResponseBody(), StandardCharsets.UTF_8);
		TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples());
		writer.flush();
		writer.close();
	    } catch (IOException e) {
		getLogger().error("Unable to export metrics via HTTP.", e);
		exchange.sendResponseHeaders(500, 0);
		exchange.close();
	    }
	}
    }

    /**
     * Handler that signals server health.
     */
    protected class HealthHandler implements HttpHandler {

	@Override
	public void handle(HttpExchange exchange) throws IOException {
	    byte[] response = "OK".getBytes("UTF-8");
	    exchange.sendResponseHeaders(200, response.length);
	    try (OutputStream out = exchange.getResponseBody()) {
		out.write(response);
	    }
	}
    }

    protected HttpServer getHttpServer() {
	return httpServer;
    }

    protected ExecutorService getHttpExecutor() {
	return httpExecutor;
    }

    protected Map<String, IMetricsHttpEndpoint> getEndpoints() {
	return endpoints;
    }
}
