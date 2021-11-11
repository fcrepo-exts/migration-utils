/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration;

import java.nio.file.Path;

/**
 * A default implementation of ObjectInfo that accepts
 * values at construction time.
 * @author mdurbin
 */
public class DefaultObjectInfo implements ObjectInfo {

    private String pid;
    private String uri;
    private Path foxmlPath;

    /**
     * the default object info
     * @param pid the pid
     * @param uri the uri
     * @param foxmlPath path to the foxml file
     */
    public DefaultObjectInfo(final String pid, final String uri, final Path foxmlPath) {
        this.pid = pid;
        this.uri = uri;
        this.foxmlPath = foxmlPath;
    }

    @Override
    public String getPid() {
        return pid;
    }

    @Override
    public String getFedoraURI() {
        return uri;
    }

    @Override
    public Path getFoxmlPath() {
        return foxmlPath;
    }
}
