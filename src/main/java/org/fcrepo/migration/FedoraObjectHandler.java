package org.fcrepo.migration;

/**
 * An interface for a class that processes Fedora 3 objects.
 * The single method {@link #processObject} would be invoked
 * for each object to be processed.
 * @author mdurbin
 */
public interface FedoraObjectHandler {

    /**
     * Invoked to process an object.  All the metadata and content
     * are accessible during this invocation.
     * @param object an object encapsulating everything about a single
     *               Fedora 3 object.
     */
    public void processObject(ObjectReference object);

}
