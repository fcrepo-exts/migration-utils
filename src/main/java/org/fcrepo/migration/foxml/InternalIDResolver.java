/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.foxml;

import java.io.IOException;

/**
 * An interface whose implementations serve as a mechanism to
 * resolve internal (to fedora/FOXML) IDs.
 * @author mdurdin
 */
public interface InternalIDResolver {

    /**
     * Gets the datastream for an internal ID.
     * @param id the internal id referenced within a FOXML file.
     * @return the binary content for the datastream referenced by the internal id
     */
    public CachedContent resolveInternalID(String id);

    /**
     * Closes any open resources.
     * @throws IOException
     */
    public void close() throws IOException;
}
