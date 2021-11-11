/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration;

import java.nio.file.Path;

/**
 * An interface defining access to the high level identifying information
 * about a fedora 3 object.
 * @author mdurbin
 */
public interface ObjectInfo {

    /**
     * @return  the pid of the object.
     */
    public String getPid();

    /**
     * @return the Fedora URI of the object (or null if none is available in
     * the source).
     */
    public String getFedoraURI();

    /**
     * @return the path to the foxml file of this object.
     */
    public Path getFoxmlPath();
}
