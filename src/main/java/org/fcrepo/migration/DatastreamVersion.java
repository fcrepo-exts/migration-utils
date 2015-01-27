package org.fcrepo.migration;

import java.io.IOException;
import java.io.InputStream;

/**
 * An interface defining access to information about a version of a
 * fedora datastream.
 */
public interface DatastreamVersion {

    /**
     * Gets the information about the datastream for which this is
     * a version.  (which in turn can be queried to get information about
     * the object).
     */
    public DatastreamInfo getDatastreamInfo();

    /**
     * Gets the id for this version.
     */
    public String getVersionId();

    /**
     * Gets the mime type for this version.
     */
    public String getMimeType();

    /**
     * Gets the label for this version.
     */
    public String getLabel();

    /**
     * Gets the date when this version was created.
     */
    public String getCreated();

    /**
     * Gets the altIDs value for this version.
     */
    public String getAltIds();

    /**
     * Gets the format URI for this version.
     */
    public String getFormatUri();

    /**
     * Gets the size (in bytes) for the content of this datastream
     * version.
     */
    public long getSize();

    /**
     * Gets the content digest (if available) for this version.
     */
    public ContentDigest getContentDigest();

    /**
     * Gets access to the content of this datastream.  When text, the
     * encoding can be expected to be UTF-8.
     * @throws IllegalStateException if invoked outside of the call
     *         to @{link FedoraObjectHandler#processDatastreamVersion}
     * @throws IOException when unable to access the stream
     */
    public InputStream getContent() throws IOException;

}
