package org.fcrepo.migration;

import java.io.InputStream;

/**
 * An interface representing all of the high-level fedora 4 operations
 * needed by the migration utility.
 *
 * @author Mike Durbin
 */
public interface Fedora4Client {

    /**
     * Determines if a resource exists.
     * @param path the path to the resource
     * @return true if it exists, false otherwise
     */
    public boolean exists(String path);

    /**
     * Creates a new resource at the given path.
     * @param path the path to the new resource
     */
    public void createResource(String path);

    /**
     * Creates a new non-RDF resource at the given path.
     * @param path the path to the new resource
     */
    public void createNonRDFResource(String path);

    /**
     * Gets the repository URL (to which paths can be appended to reference resources).
     * @return the repository URL
     */
    public String getRepositoryUrl();

    /**
     * Creates or updates a non-RDF resource that points to external content at the given URL.
     * @param path the path of the resource to be created
     * @param url the URL at which the external content is hosted
     */
    public void createOrUpdateRedirectNonRDFResource(String path, String url);

    /**
     * Creates or updates a non-RDF resource.
     * @param path the path of the resource to be modified/created
     * @param content the non-RDF content
     * @param contentType the mime type of the content
     */
    public void createOrUpdateNonRDFResource(String path, InputStream content, String contentType);

    /**
     * Creates a version snapshot for the resource (or graph) at the given path.
     * @param path the path of the resource to be versioned
     * @param versionId a label for the version
     */
    public void createVersionSnapshot(String path, String versionId);

    /**
     * Updates properties on a resource.
     * @param path the resource whose properties are to be updated.
     * @param sparqlUpdate the sparql update statements to be applied
     */
    public void updateResourceProperties(String path, String sparqlUpdate);

    /**
     * Updates properties on a non-RDF resource.
     * @param path the resource whose properties are to be updated.
     * @param sparqlUpdate the sparql update statements to be applied
     */
    public void updateNonRDFResourceProperties(String path, String sparqlUpdate);
}
