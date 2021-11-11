/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.foxml;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * A CashedContent implementation that exposes content stored in a
 * file.
 * @author mdurbin
 */
public class FileCachedContent implements CachedContent {

    private File file;

    /**
     * File cached content
     * @param file the file
     */
    public FileCachedContent(final File file) {
        this.file = file;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        if (!file.exists()) {
            throw new IllegalStateException("Cached content is not available.");
        }
        return new BufferedInputStream(new FileInputStream(file));
    }

    @Override
    public Optional<File> getFile() {
        return Optional.ofNullable(file);
    }
}
