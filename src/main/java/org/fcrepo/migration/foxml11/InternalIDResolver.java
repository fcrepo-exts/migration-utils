package org.fcrepo.migration.foxml11;

/**
 * An interface whose implementations serve as a mechanism to
 * resolve internal (to fedora/FOXML) IDs.
 */
public interface InternalIDResolver {

    public CachedContent resolveInternalID(String id);
}
