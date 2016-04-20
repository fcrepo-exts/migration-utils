/*
 * Copyright 2015 DuraSpace, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    /**
     * Creates a placeholder resource at the given path (or at a server-assigned path,
     * if no path is given) if no resource exists at that path.  If a resource already
     * exists, this method returns the path to that resource which may or may not be
     * a placeholder.  If none exists, this method creates a new resource that should
     * should be distinguishable from resources that have already been migrated as well
     * as resources created using another process.
     * @param path a path at which to create a placeholder resource (or null to create
     *             a placeholder resource at a server-assigned path).
     * @return the path of the placeholder resource that was created
     */
    public String createPlaceholder(String path);

    /**
     * Creates a placeholder non-RDF resource at the given path (or at a server-assigned
     * path, if no path is given) if no resource exists at that path.  If a resource
     * already exists, this method returns the path to that resource which may or may not
     * be a placeholder.  If none exists, this method creates a new resource that should
     * should be distinguishable from resources that have already been migrated as well
     * as resources created using another process.
     * @param path a path at which to create a placeholder resource (or null to create
     *             a placeholder resource at a server-assigned path).
     * @return the path of the placeholder resource that was created
     */
    public String createNonRDFPlaceholder(String path);

    /**
     * Determines whether the resource at the given path is a placeholder or not.
     * @param path a path of a resource (expected to exist)
     * @return true if it's a placeholder, false otherwise
     */
    public boolean isPlaceholder(String path);
}
