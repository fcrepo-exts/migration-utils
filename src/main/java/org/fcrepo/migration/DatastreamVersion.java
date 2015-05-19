package org.fcrepo.migration;

import java.io.IOException;
import java.io.InputStream;

/**
 * An interface defining access to information about a version of a
 * fedora datastream.
 * @author mdurbin
 */
public interface DatastreamVersion {

    /**
     * Gets the information about the datastream for which this is
     * a version.  (which in turn can be queried to get information about
     * the object).
     *
     * @return {@link org.fcrepo.migration.DatastreamInfo}
     */
    public DatastreamInfo getDatastreamInfo();

    /**
     * Gets the id for this version.
     *
     * @return version id
     */
    public String getVersionId();

    /**
     * Gets the mime type for this version.
     *
     * @return mime-type
     */
    public String getMimeType();

    /**
     * Gets the label for this version.
     *
     * @return label
     */
    public String getLabel();

    /**
     * Gets the date when this version was created.
     *
     * @return creation date
     */
    public String getCreated();

    /**
     * Gets the altIDs value for this version.
     *
     * @return alternate IDs
     */
    public String getAltIds();

    /**
     * Gets the format URI for this version.
     *
     * @return format URI
     */
    public String getFormatUri();

    /**
     * Gets the size (in bytes) for the content of this datastream
     * version.
     *
     * @return size
     */
    public long getSize();

    /**
     * Gets the content digest (if available) for this version.
     *
     * @return {@link org.fcrepo.migration.ContentDigest}
     */
    public ContentDigest getContentDigest();

    /**
     * Gets access to the content of this datastream.  When text, the
     * encoding can be expected to be UTF-8.
     *
     * @return {@link java.io.InputStream of content}
     * @throws IllegalStateException if invoked outside of the call
     *         to @{link StreamingFedoraObjectHandler#processDatastreamVersion}
     * @throws IOException when unable to access the stream
     */
    public InputStream getContent() throws IOException;

    /**
     * Returns the URL to which an External (X) or Redirect (R) datastream
     * points.  Throws IllegalStateException if this isn't an external or
     * redirect datastream.
     *
     * @return URL of datastream
     */
    public String getExternalOrRedirectURL();

    /**
     * Determines if this is the first version of a datastream.
     *
     * @param obj to be tested whether is first version
     *
     * @return  True if this is the first version, false otherwise.
     */
    public boolean isFirstVersionIn(ObjectReference obj);

    /**
     * Determines if this is the last version of a datastream.
     *
     * @param obj to be tested whether is last version
     *
     * @return  True if this is the last version, false otherwise.
     */
    public boolean isLastVersionIn(ObjectReference obj);
}
