package org.fcrepo.migration;

/**
 * A default implementation of ContentDigest that accepts
 * values at construction time.
 * @author mdurbin
 */
public class DefaultContentDigest implements ContentDigest {

    private String type;

    private String digest;

    /**
     * default content digest.
     * @param type the type
     * @param digest the digest
     */
    public DefaultContentDigest(final String type, final String digest) {
        this.type = type;
        this.digest = digest;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String getDigest() {
        return digest;
    }
}
