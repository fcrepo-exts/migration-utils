package org.fcrepo.migration.foxml11;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A CachedContent implementation that stores the entirety of the content
 * in memory as String.
 */
public class MemoryCachedContent implements CachedContent {

    private String content;

    public MemoryCachedContent(String content) {
        this.content = content;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return new ByteArrayInputStream(content.getBytes("UTF-8"));
    }
}
