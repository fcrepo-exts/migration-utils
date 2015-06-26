package org.fcrepo.migration;

import java.util.List;

/**
 * A reference to a Fedora 3 object.  Through the methods, all metadata and datastream
 * content is available.
 * @author mdurbin
 */
public interface ObjectReference {

    /**
     * @return all the basic object information.
     */
    public ObjectInfo getObjectInfo();

    /**
     * @return all the object properties.
     */
    public ObjectProperties getObjectProperties();

    /**
     * Lists all datastream ids.
     * @return all datastream ids
     */
    public List<String> listDatastreamIds();

    /**
     * Gets all versions of the datastream with the given id, from oldest to newest.
     * @param datastreamId the id (a value returned from listDatastreams()) of the
     *                     datastream whose versions are being requested.
     * @return a list of datastream versions ordered from oldest to newest.
     */
    public List<DatastreamVersion> getDatastreamVersions(String datastreamId);

    /**
     * @return true if the underlying object had a Fedora2-style disseminator
     *         that was lost in the migration.
     */
    public boolean hadFedora2Disseminators();
}
