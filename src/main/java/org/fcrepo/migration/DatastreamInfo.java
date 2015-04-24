package org.fcrepo.migration;

/**
 * An interface defining access to information about a fedora datastream.
 * @author mdurbin
 */
public interface DatastreamInfo {

    /**
     * Gets the information about the object to which this datastream
     * belongs.
     */
    public ObjectInfo getObjectInfo();

    /**
     * Gets the identifier for this datastream (Unique within an object).
     */
    public String getDatastreamId();

    /**
     * Gets the control group for this datastream.  This is expected to be
     * "M", "X", "R" or "E".
     */
    public String getControlGroup();

    /**
     * Gets the fedora URI for this datastream.
     */
    public String getFedoraURI();

    /**
     * Gets the state for this datastream.  This is expected to be "A", "I" or
     * "D".
     */
    public String getState();

    /**
     * Returns true if this datastream was/is versionable.
     */
    public boolean getVersionable();
}
