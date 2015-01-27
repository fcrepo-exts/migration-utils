package org.fcrepo.migration.foxml11;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A CashedContent implementation that exposes content stored in a
 * file.
 */
public class FileCachedContent implements CachedContent {

    private File file;

    public FileCachedContent(File file) {
        this.file = file;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        if (!file.exists()) {
            throw new IllegalStateException("Cached content is not available.");
        }
        return new FileInputStream(file);
    }
}
