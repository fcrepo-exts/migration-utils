package org.fcrepo.migration.foxml11;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class HttpClientURLFetcher implements URLFetcher {

    CloseableHttpClient httpClient;

    public HttpClientURLFetcher() {
        httpClient = HttpClients.createDefault();
    }

    @Override
    public InputStream getContentAtUrl(URL url) throws IOException {
        return httpClient.execute(new HttpGet(String.valueOf(url))).getEntity().getContent();

    }
}
