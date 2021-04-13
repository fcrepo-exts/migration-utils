/*
 * Copyright 2021 DuraSpace, Inc.
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
package org.fcrepo.migration.metrics;

import static java.net.HttpURLConnection.HTTP_OK;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

/**
 * Simple actuator for publishing metrics for Prometheus
 *
 * @author mikejritter
 */
public class PrometheusActuator {

    private final ExecutorService executor;

    private HttpServer server;
    private PrometheusMeterRegistry registry;

    public PrometheusActuator(final boolean enableMetrics) {
        if (enableMetrics) {
            configureRegistry();
            configureServer();
        }
        this.executor = Executors.newSingleThreadExecutor();
    }

    private void configureRegistry() {
        registry = new PrometheusMeterRegistry(new PrometheusConfig() {
            @Override
            public Duration step() {
                return Duration.ofSeconds(10);
            }

            @Override
            public String get(final String key) {
                return null;
            }
        });

        registry.config().meterFilter(new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(final Meter.Id id,
                                                         final DistributionStatisticConfig config) {
                if (id.getType() == Meter.Type.TIMER) {
                    return new DistributionStatisticConfig.Builder()
                        .percentilesHistogram(true)
                        .percentiles(0.90, 0.95, 0.99)
                        .build().merge(config);
                }
                return config;
            }
        });

        new JvmGcMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new ClassLoaderMetrics().bindTo(registry);

        Metrics.addRegistry(registry);
    }

    private void configureServer() {
        try {
            server = HttpServer.create(new InetSocketAddress(8080), 0);
        } catch (IOException e) {
            throw new RuntimeException("Unable to start http server for publishing metrics!", e);
        }

        server.setExecutor(executor);
        server.createContext("/prometheus", handler -> {
            final var response = registry.scrape();
            handler.sendResponseHeaders(HTTP_OK, response.getBytes().length);
            try (final var os = handler.getResponseBody()) {
                os.write(response.getBytes());
            }
        });
    }

    /**
     * Start the HTTP server for metric publishing
     *
     * @throws RuntimeException if the server cannot be started
     */
    public void start() {
        if (server != null) {
            server.start();
        }
    }

    /**
     * Stop the HTTP server
     */
    public void stop() {
        if (server != null) {
            server.stop(0);
        }
        executor.shutdown();
    }

}
