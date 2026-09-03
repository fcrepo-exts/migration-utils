/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.migration.handlers.ocfl;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

/**
 * @author Dan Field
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
