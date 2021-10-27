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

/**
 * A simple abstraction around the functionality of reading
 * content from a URL as an InputStream.  Use of this interface
 * allows for pluggable implementations and easier testing.
 * @author mdurbin
 */
public interface URLFetcher {

    /**
     * get content from a url.
     * @param url the url
     * @return the content
     * @throws IOException IO exception
     */
    public InputStream getContentAtUrl(URL url) throws IOException;
}
