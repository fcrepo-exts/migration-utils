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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.Test;

/**
 * @author Claude
 */
public class PrometheusActuatorTest {

    @Test
    public void disabledActuatorDoesNothing() {
        final var actuator = new PrometheusActuator(false);
        // start/stop should be no-ops when metrics are disabled (no server was created)
        actuator.start();
        actuator.stop();
    }

    @Test
    public void enabledActuatorServesScrape() throws IOException {
        final var actuator = new PrometheusActuator(true);
        try {
            // Exercise both branches of the meter filter (timer and non-timer).
            Timer.builder("test.timer").register(Metrics.globalRegistry).record(() -> { });
            Counter.builder("test.counter").register(Metrics.globalRegistry).increment();

            actuator.start();

            final var url = new URL("http://localhost:8080/prometheus");
            final var connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());

            final String body;
            try (InputStream is = connection.getInputStream()) {
                body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }
            assertTrue("scrape output should contain registered metrics", body.contains("jvm_"));
        } finally {
            actuator.stop();
        }
    }
}
