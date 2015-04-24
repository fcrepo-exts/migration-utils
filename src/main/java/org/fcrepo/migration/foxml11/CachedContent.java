package org.fcrepo.migration.foxml11;

import java.io.IOException;
import java.io.InputStream;

/**
 * An interface representing content that is accessible as an InputStream.
 * @author mdurbin
 */
public interface CachedContent {

    /**
     * get input stream.
     * @return the input stream
     * @throws IOException IO exception
     */
    public InputStream getInputStream() throws IOException;

}
