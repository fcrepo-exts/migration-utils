package org.fcrepo.migration;

/**
 * A default implementation of ObjectInfo that accepts
 * values at construction time.
 * @author mdurbin
 */
public class DefaultObjectInfo implements ObjectInfo {

    private String pid;

    private String uri;

    /**
     * the default object info
     * @param pid the pid
     * @param uri the uri
     */
    public DefaultObjectInfo(final String pid, final String uri) {
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
