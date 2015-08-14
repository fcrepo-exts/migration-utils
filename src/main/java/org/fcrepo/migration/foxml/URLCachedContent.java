/**
 * Copyright 2015 DuraSpace, Inc.
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
package org.fcrepo.migration.foxml;

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
