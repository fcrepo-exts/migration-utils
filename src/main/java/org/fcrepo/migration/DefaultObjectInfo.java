package org.fcrepo.migration;

import java.io.InputStream;

/**
 * A default implementation of ObjectInfo that accepts
 * values at construction time.
 */
public class DefaultObjectInfo implements ObjectInfo {

    private String pid;

    private String uri;

    public DefaultObjectInfo(String pid, String uri) {
        this.pid = pid;
        this.uri = uri;
    }

    @Override
    public String getPid() {
        return pid;
    }

    @Override
    public String getFedoraURI() {
        return uri;
    }

}
