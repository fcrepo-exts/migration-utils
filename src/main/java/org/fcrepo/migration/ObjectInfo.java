package org.fcrepo.migration;

/**
 * An interface defining access to the high level identifying information
 * about a fedora 3 object.
 * @author mdurbin
 */
public interface ObjectInfo {

    /**
     * Returns the pid of the object.
     */
    public String getPid();

    /**
     * Gets the Fedora URI of the object (or null if none is available in
     * the source).
     */
    public String getFedoraURI();

}
