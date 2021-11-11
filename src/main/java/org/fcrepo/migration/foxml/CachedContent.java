/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.foxml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

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

    /**
     * get the file backing the CachedContent if it exists
     * @return the file
     */
    default Optional<File> getFile() {
        return Optional.empty();
    }
}
