package org.fcrepo.migration;

/**
 * A default implementation of ContentDigest that accepts
 * values at construction time.
 */
public class DefaultContentDigest implements ContentDigest {

    private String type;

    private String digest;

    public DefaultContentDigest(String type, String digest) {
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
