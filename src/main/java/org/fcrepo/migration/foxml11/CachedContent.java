package org.fcrepo.migration.foxml11;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

/**
 * An interface representing content that is accessible as an InputStream.
 */
public interface CachedContent {

    public InputStream getInputStream() throws IOException;

}
