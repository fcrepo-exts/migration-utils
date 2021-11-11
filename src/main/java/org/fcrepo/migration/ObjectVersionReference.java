/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
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
     * @return the ObjectReference object that encapsulates everything about
     * the underlying Fedora 3 object.
     */
    public ObjectReference getObject();

    /**
     * @return all the object properties.  This is unversioned information.
     */
    public ObjectProperties getObjectProperties();

    /**
     * @return the lastModifiedDate proeperty for this version.  This is formatted as
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
     * @return true if this is the first version.
     */
    public boolean isLastVersion();

    /**
     * @return true if this is the last version.
     */
    public boolean isFirstVersion();

    /**
     * @return the version index (0 for first, 1 for second, etc.) in chronological
     * order from oldest to newest.
     */
    public int getVersionIndex();

    /**
     * @param dsId of datastream to be tested for change.
     *
     * @return true if datastream with the given DSID changed as part of the
     * update that contributed to this version.
     */
    public boolean wasDatastreamChanged(String dsId);
}
