/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.foxml;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
/**
 *
 * @author mdurbin
 *
 */
public class HttpClientURLFetcher implements URLFetcher {

    CloseableHttpClient httpClient;

    /**
     * Http Client URL fetcher.
     */
    public HttpClientURLFetcher() {
        httpClient = HttpClients.createDefault();
    }

    @Override
    public InputStream getContentAtUrl(final URL url) throws IOException {
        // The response must stay open until the caller has finished reading the content, so the returned
        // stream takes ownership of the response and closes it when the stream itself is closed.
        final ClassicHttpResponse response = httpClient.executeOpen(null, new HttpGet(String.valueOf(url)), null);
        try {
            final HttpEntity entity = response.getEntity();
            if (entity == null) {
                throw new IOException("No content returned from " + url);
            }
            return new FilterInputStream(entity.getContent()) {
                @Override
                public void close() throws IOException {
                    try {
                        super.close();
                    } finally {
                        response.close();
                    }
                }
            };
        } catch (final IOException | RuntimeException e) {
            response.close();
            throw e;
        }
    }
}
