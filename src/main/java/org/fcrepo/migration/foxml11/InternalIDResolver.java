package org.fcrepo.migration.foxml11;

/**
 * An interface whose implementations serve as a mechanism to
 * resolve internal (to fedora/FOXML) IDs.
 * @author mdurdin
 */
public interface InternalIDResolver {

    /**
     * resolve internal ID.
     * @param id the internal ID
     * @return the cached content
     */
    public CachedContent resolveInternalID(String id);
}
