package org.fcrepo.migration.foxml11;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * A CachedContent implementation that exposes content stored
 * at a resolvable URL.
 */
public class URLCachedContent implements CachedContent {

    private URL url;

    private URLFetcher fetcher;

    public URLCachedContent(URL url, URLFetcher fetcher) {
        this.fetcher = fetcher;
        this.url = url;
    }
    
    public URL getURL() {
        return url;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return fetcher.getContentAtUrl(url);
    }
}
