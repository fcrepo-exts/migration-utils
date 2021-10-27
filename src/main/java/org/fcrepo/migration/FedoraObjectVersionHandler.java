/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration;

/**
 * An interface for a class that processes Fedora 3 objects
 * as an iteration of versions.  The single method
 * {@link #processObjectVersions} would be invoked once for
 * an object providing the entire version history (ie. times
 * with identifiable changes) in the Fedora 3 object
 * starting from the creation and proceeding chronologically
 * @author mdurbin
 */
public interface FedoraObjectVersionHandler {

    /**
     * Invoked to process a version of a Fedora 3 object.  All the metadata
     * and content that changed from the previous version to the one
     * represented by the current call is conventiently made available.
     * @param versions an iterable of Objects each encapsulating everything
     *               about a single version of a Fedora 3 object.
     * @param objectInfo information about the Fedora 3 object being processed
     */
    public void processObjectVersions(Iterable<ObjectVersionReference> versions, final ObjectInfo objectInfo);
}
