package org.fcrepo.migration;

/**
 * An interface for a class that processes Fedora 3 objects 
 * one version at a time.  The single method 
 * {@link #processObjectVersion} would be invoked once for
 * each version (identifiable change) in the Fedora 3 object
 * starting from the creation and proceding chronologically
 */
public interface FedoraObjectVersionHandler {

    /**
     * Invoked to process a version of a Fedora 3 object.  All the metadata
     * and content that changed from the previous version to the one
     * represented by the current call is conventiently made available.
     * @param reference an object encapsulating everything about a single
     *               version of a Fedora 3 object.
     */
    public void processObjectVersion(ObjectVersionReference reference);
}
