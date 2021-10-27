/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration;

/**
 * An interface with methods that are meant to be invoked when processing
 * a fedora 3 object such that every bit of information in that fedora 3
 * object is exposed to the instance implementing this interface.
 *
 * Instances of this class are expected to be used for a single fedora
 * object, and method calls should not require implementations to maintain
 * state.
 * @author mdurbin
 */
public interface StreamingFedoraObjectHandler {

    /**
     * begin object.
     * @param object the object info
     */
    public void beginObject(ObjectInfo object);

    /**
     * Invoked to allow processing of properties by this StreamingFedoraObjectHandler.
     * @param properties the properties for the object
     */
    public void processObjectProperties(ObjectProperties properties);

    /**
     * Invoked to allow processing of a datastream by this StreamingFedoraObjectHandler.
     * @param dsVersion an encapsulation of the datastream version.  References to this object must
     *                  not be used after completeObject() or abortObject() have completed as
     *                  the resources exposed may no longer be available.
     */
    public void processDatastreamVersion(DatastreamVersion dsVersion);

    /**
     * A hook called after the object has been completely processed.  This may be useful for any cleanup or
     * finalization routines.  Furthermore, once this method invocation is complete, any references
     * provided to prior calls will no longer be in scope.
     *
     * @param object to be completed.
     */
    public void completeObject(ObjectInfo object);

    /**
     * Invoked if processing of the object failed for some reason.
     *
     * @param object to be aborted.
     */
    public void abortObject(ObjectInfo object);
}
