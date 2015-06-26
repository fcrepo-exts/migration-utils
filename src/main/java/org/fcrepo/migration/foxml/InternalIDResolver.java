package org.fcrepo.migration.foxml;

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
}
