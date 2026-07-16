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
package org.fcrepo.migration.handlers.ocfl;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

/**
 * @author Claude
 */
public class CountingInputStreamTest {

    @Test
    public void readsSingleBytesToEof() throws IOException {
        final var data = "abc".getBytes(StandardCharsets.UTF_8);
        try (var stream = new CountingInputStream(new ByteArrayInputStream(data))) {
            int count = 0;
            while (stream.read() != -1) {
                count++;
            }
            assertEquals(3, count);
        }
    }

    @Test
    public void readsByteArraysToEof() throws IOException {
        final var data = "hello world".getBytes(StandardCharsets.UTF_8);
        try (var stream = new CountingInputStream(new ByteArrayInputStream(data))) {
            final var buffer = new byte[4];
            int total = 0;
            int read;
            while ((read = stream.read(buffer, 0, buffer.length)) != -1) {
                total += read;
            }
            assertEquals(data.length, total);
        }
    }
}
