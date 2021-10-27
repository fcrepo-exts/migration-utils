/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration;

/**
 * An interface defining access to information about a fedora datastream's
 * content digest.
 * @author mdurbin
 */
public interface ContentDigest {

    /**
     * Gets the type: one of several defined in the fedora foxml schema.
     *
     * @return string containing the type
     */
    public String getType();

    /**
     * Gets the value of the content digest.
     * @return content digest
     */
    public String getDigest();
}
