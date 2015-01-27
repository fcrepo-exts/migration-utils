package org.fcrepo.migration.foxml11;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * A simple abstraction around the functionality of reading
 * content from a URL as an InputStream.  Use of this interface
 * allows for pluggable implementations and easier testing.
 */
public interface URLFetcher {

    public InputStream getContentAtUrl(URL url) throws IOException;
}
