package org.fcrepo.migration.foxml11;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A CachedContent implementation that stores the entirety of the content
 * in memory as String.
 * @author mdurbin
 */
public class MemoryCachedContent implements CachedContent {

    private String content;

    /**
     * memory cached content.
     * @param content the content
     */
    public MemoryCachedContent(final String content) {
        this.content = content;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return new ByteArrayInputStream(content.getBytes("UTF-8"));
    }
}
