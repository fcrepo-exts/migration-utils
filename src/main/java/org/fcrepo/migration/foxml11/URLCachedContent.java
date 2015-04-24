package org.fcrepo.migration.foxml11;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * A CachedContent implementation that exposes content stored
 * at a resolvable URL.
 * @author mdurbin
 */
public class URLCachedContent implements CachedContent {

    private URL url;

    private URLFetcher fetcher;

    /**
     * url cached content.
     * @param url the url
     * @param fetcher the fetcher
     */
    public URLCachedContent(final URL url, final URLFetcher fetcher) {
        this.fetcher = fetcher;
        this.url = url;
    }
    /**
     * get URL.
     * @return the url
     */
    public URL getURL() {
        return url;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return fetcher.getContentAtUrl(url);
    }
}
