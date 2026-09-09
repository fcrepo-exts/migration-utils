/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.migration.foxml;

import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Exercises {@link HttpClientURLFetcher} against a local HTTP server.
 *
 * @author Dan Field
 */
public class HttpClientURLFetcherTest {

    private static final String BODY = "some datastream content";

    private HttpServer server;
    private HttpClientURLFetcher fetcher;

    @Before
    public void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/content", handler -> {
            final var bytes = BODY.getBytes(StandardCharsets.UTF_8);
            handler.sendResponseHeaders(HTTP_OK, bytes.length);
            try (final var os = handler.getResponseBody()) {
                os.write(bytes);
            }
        });
        server.createContext("/empty", handler -> {
            handler.sendResponseHeaders(HTTP_NO_CONTENT, -1);
            handler.close();
        });
        server.start();
        fetcher = new HttpClientURLFetcher();
    }

    @After
    public void tearDown() {
        server.stop(0);
    }

    @Test
    public void shouldReturnAStreamThatIsStillReadableAfterTheFetchReturns() throws IOException {
        // The stream is read lazily by the caller rather than inside getContentAtUrl(), so the
        // underlying response has to stay open until the returned stream is closed.
        final var stream = fetcher.getContentAtUrl(url("/content"));
        assertEquals(BODY, IOUtils.toString(stream, StandardCharsets.UTF_8));
        stream.close();
    }

    @Test
    public void shouldReleaseTheResponseWhenTheContentCannotBeRead() {
        // A 204 carries no entity, so the fetch fails after the response has been opened.
        assertThrows(Exception.class, () -> fetcher.getContentAtUrl(url("/empty")));
    }

    private java.net.URL url(final String path) throws IOException {
        return URI.create("http://localhost:" + server.getAddress().getPort() + path).toURL();
    }
}
