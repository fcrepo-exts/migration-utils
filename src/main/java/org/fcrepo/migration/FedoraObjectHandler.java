package org.fcrepo.migration;

import java.io.IOException;

/**
 * An interface with methods that are meant to be invoked when processing
 * a fedora 3 object such that every bit of information in that fedora 3
 * object is exposed to the instance implementing this interface.
 *
 * Instances of this class are expected to be used for a single fedora
 * object, and method calls should not require implementations to maintain
 * state.
 */
public interface FedoraObjectHandler {

    public void beginObject(ObjectInfo object);

    /**
     * Invoked to allow processing of properties by this FedoraObjectHandler.
     * @param properties the properties for the object
     */
    public void processObjectProperties(ObjectProperties properties);

    /**
     * Invoked to allow processing of a datastream by this FedoraObjectHandler.
     * @param dsVersion an encapsulation of the datastream version.  References to this object must
     *                  not be retained or referenced outside of implementations of this method as
     *                  attached resources (cached files, references to streams) may be updated and
     *                  no longer valid.
     */
    public void processDatastreamVersion(DatastreamVersion dsVersion);

    /**
     * A hook called after the object has been completely processed.  This may be useful for any cleanup or
     * finalization routines.  Furthermore, once this method invokation is complete, any references
     * provided to prior calls will no longer be in scope.
     */
    public void completeObject(ObjectInfo object);

    /**
     * Invoked if processing of the object failed for some reason.
     */
    public void abortObject(ObjectInfo object);
}
