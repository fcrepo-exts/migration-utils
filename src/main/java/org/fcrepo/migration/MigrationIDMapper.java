package org.fcrepo.migration;

/**
 * An interface whose implementations represent methods to 
 * convert Fedora 3 PIDs into fedora 4 paths. 
 */
public interface MigrationIDMapper {

    /**
     * Takes a Fedora 3 Object reference and returns the path 
     * that object would have in Fedora 4.
     * @param object an ObjectReference for a Fedora 3 object.
     * @return a path suitable for use in Fedora 4.
     */
    public String mapObjectPath(ObjectReference object);

    /**
     * Takes a Fedora 3 DatastreamInfo object and returns the path
     * that datastream would have in Fedora 4.
     * @param dsInfo a DatastreamInfo for a Fedora 3 datastream
     * @return a path suitable for use in Fedora 4.
     */
    public String mapDatastreamPath(DatastreamInfo dsInfo);
    
}
