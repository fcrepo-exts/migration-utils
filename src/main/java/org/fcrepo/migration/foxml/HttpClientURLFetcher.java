/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.foxml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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
        return httpClient.execute(new HttpGet(String.valueOf(url))).getEntity().getContent();

    }
}
