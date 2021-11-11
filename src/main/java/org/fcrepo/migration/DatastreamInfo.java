/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration;

/**
 * An interface defining access to information about a fedora datastream.
 * @author mdurbin
 */
public interface DatastreamInfo {

    /**
     * Gets the information about the object to which this datastream
     * belongs.
     *
     * @return {@link org.fcrepo.migration.ObjectInfo}
     */
    public ObjectInfo getObjectInfo();

    /**
     * Gets the identifier for this datastream (Unique within an object).
     *
     * @return datastream id
     */
    public String getDatastreamId();

    /**
     * Gets the control group for this datastream.  This is expected to be
     * "M", "X", "R" or "E".
     *
     * @return control group
     */
    public String getControlGroup();

    /**
     * Gets the fedora URI for this datastream.
     *
     * @return Fedora URI
     */
    public String getFedoraURI();

    /**
     * Gets the state for this datastream.  This is expected to be "A", "I" or
     * "D".
     *
     * @return  state
     */
    public String getState();

    /**
     * Returns true if this datastream was/is versionable.
     *
     * @return true if versionable
     */
    public boolean getVersionable();
}
