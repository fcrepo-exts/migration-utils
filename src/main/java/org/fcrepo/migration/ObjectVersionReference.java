package org.fcrepo.migration;

import java.util.List;

/**
 * Represents a version of a Fedora 3 object.
 *
 * TODO: perhaps the audit trail should be parsed and exposed here
 * @author mdurbin
 */
public interface ObjectVersionReference {

    /**
     * Gets the ObjectReference object that encapsualtes everything about
     * the underlying Fedora 3 object.
     */
    public ObjectReference getObject();

    /**
     * Gets all the basic object information.  This is unversioned information.
     */
    public ObjectInfo getObjectInfo();

    /**
     * Gets all the object properties.  This is unversioned information.
     */
    public ObjectProperties getObjectProperties();

    /**
     * Gets the lastModifiedDate proeperty for this version.  This is formatted as
     * all Fedora 3 dates are formatted.
     */
    public String getVersionDate();

    /**
     * Lists the current version of all datastreams changed from the pervious version
     * to this one.
     * @return a List containing a DatastreamVersion for each datastream that changed
     * from the last version to this one.
     */
    public List<DatastreamVersion> listChangedDatastreams();

    /**
     * Indicates whether this is the first version.
     */
    public boolean isLastVersion();

    /**
     * Indicates whether this is the last version.
     */
    public boolean isFirstVersion();

    /**
     * Gets the version index (0 for first, 1 for second, etc.) in chronological
     * order from oldest to newest.
     */
    public int getVersionIndex();

    /**
     * Determines whether a datastream with the given DSID changed as part of the
     * update that contributed to this version.
     */
    public boolean wasDatastreamChanged(String dsId);
}
