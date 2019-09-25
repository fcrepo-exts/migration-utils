
package org.fcrepo.migration.f4clients;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.fcrepo.migration.Fedora4Client;
import org.slf4j.Logger;

import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapper;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapperBuilder;
import edu.wisc.library.ocfl.core.storage.FileSystemOcflStorage;

/**
 * @author RichardDWilliams
 * @since 2019-09-18
 */
public class OCFLFedora4Client implements Fedora4Client {

    private static final Logger LOGGER = getLogger(OCFLFedora4Client.class);

    private final OcflRepository ocflRepo;

    private final String storageRoot;

    private final String stagingRoot;

    public enum ObjectIdMapperType {
        FLAT
    }

    /**
     * Constructor
     *
     * @author Remigiusz Malessa
     * @since 4.4.1-SNAPSHOT
     * @param storage Root for OCFL Objects
     * @param staging directory for in-progress OCFL Objects
     * @param mapper to be used to map object id's to paths
     */
    public OCFLFedora4Client(final String storage, final String staging, final ObjectIdMapperType mapper) {

        this.storageRoot = storage;
        this.stagingRoot = staging;

        ObjectIdPathMapper objectIdPathMapper = null;
        switch (mapper) {
        case FLAT:
            objectIdPathMapper = new ObjectIdPathMapperBuilder().withDefaultCaffeineCache().buildFlatMapper();
            break;
        default:
            throw new RuntimeException("No implementation for the maper: " + mapper);
        }

        // If locations exist instantiate repository
        if (new File(storage).isDirectory() && new File(staging).isDirectory()) {
            LOGGER.debug("OCFLFedora4Client: {}, {}", storage, staging);
            final Path repoDir = Paths.get(this.storageRoot);
            ocflRepo =
                    new OcflRepositoryBuilder().build(new FileSystemOcflStorage(repoDir,
                            objectIdPathMapper), repoDir.resolve("deposit"));
        } else {
            final String missingDir = new File(storage).isDirectory() ? staging : storage;
            throw new RuntimeException("Failed to create OCFL repository, because location: " + missingDir +
                    " does not exists, or is not a directory.");
        }
    }

    /**
     * This method returns true if the resource exists in the storage root
     *
     * @author Remigiusz Malessa (based on implementation by awoods)
     * @since 4.4.1-SNAPSHOT
     * @param path to the resource
     * @return true if resource exists in 'storage' root
     */
    @Override
    public boolean exists(final String path) {

        final boolean exists = ocflRepo.containsObject(path);
        LOGGER.debug("Object with path: {}, exists: {}", path, exists);
        return exists;
    }

    @Override
    public void createResource(final String path) {
        LOGGER.info("to-be-implemented: createResource: " + path);
    }

    /**
     * Gets the repository URL (to which paths can be appended to reference resources).
     *
     * @return the repository URL
     */
    @Override
    public String getRepositoryUrl() {
        LOGGER.info("to-be-implemented: getRepositoryUrl");
        return null;
    }

    /**
     * Creates or updates a non-RDF resource that points to external content at the given URL.
     *
     * @param path the path of the resource to be created
     * @param url the URL at which the external content is hosted
     */
    @Override
    public void createOrUpdateRedirectNonRDFResource(final String path, final String url) {
        LOGGER.info("to-be-implemented: createOrUpdateRedirectNonRDFResource: " + path + ", " + url);
    }

    /**
     * Creates or updates a non-RDF resource.
     *
     * @param path the path of the resource to be modified/created
     * @param content the non-RDF content
     * @param contentType the mime type of the content
     */
    @Override
    public void createOrUpdateNonRDFResource(final String path, final InputStream content, final String contentType) {
        LOGGER.info("to-be-implemented: createOrUpdateNonRDFResource: " + path + ", " + contentType);
    }

    /**
     * Creates a version snapshot for the resource (or graph) at the given path.
     *
     * @param path the path of the resource to be versioned
     * @param versionId a label for the version
     */
    @Override
    public void createVersionSnapshot(final String path, final String versionId) {
        LOGGER.info("to-be-implemented: createVersionSnapshot: {}, version-id: {}", path, versionId);
    }

    /**
     * Updates properties on a resource.
     *
     * @param path the resource whose properties are to be updated.
     * @param sparqlUpdate the sparql update statements to be applied
     */
    @Override
    public void updateResourceProperties(final String path, final String sparqlUpdate) {
        LOGGER.info("to-be-implemented: updateResourceProperties: " + path + ", " + sparqlUpdate);
    }

    /**
     * Updates properties on a non-RDF resource.
     *
     * @param path the resource whose properties are to be updated.
     * @param sparqlUpdate the sparql update statements to be applied
     */
    @Override
    public void updateNonRDFResourceProperties(final String path, final String sparqlUpdate) {
        LOGGER.info("to-be-implemented: updateNonRDFResourceProperties: " + path + ", " + sparqlUpdate);
    }

    /**
     * Creates a placeholder resource at the given path (or at a server-assigned path, if no path is given) if no
     * resource exists at that path. If a resource already exists, this method returns the path to that resource which
     * may or may not be a placeholder. If none exists, this method creates a new resource that should should be
     * distinguishable from resources that have already been migrated as well as resources created using another
     * process.
     *
     * @param path a path at which to create a placeholder resource (or null to create a placeholder resource at a
     *        server-assigned path).
     * @return the path of the placeholder resource that was created
     */
    @Override
    public String createPlaceholder(final String path) {
        LOGGER.info("to-be-implemented: createPlaceholder: " + path);
        return null;
    }

    /**
     * Creates a placeholder non-RDF resource at the given path (or at a server-assigned path, if no path is given) if
     * no resource exists at that path. If a resource already exists, this method returns the path to that resource
     * which may or may not be a placeholder. If none exists, this method creates a new resource that should should be
     * distinguishable from resources that have already been migrated as well as resources created using another
     * process.
     *
     * @param path a path at which to create a placeholder resource (or null to create a placeholder resource at a
     *        server-assigned path).
     * @return the path of the placeholder resource that was created
     */
    @Override
    public String createNonRDFPlaceholder(final String path) {
        LOGGER.info("to-be-implemented: createNonRDFPlaceholder: " + path);
        return null;
    }

    /**
     * Determines whether the resource at the given path is a placeholder or not.
     *
     * @param path a path of a resource (expected to exist)
     * @return true if it's a placeholder, false otherwise
     */
    @Override
    public boolean isPlaceholder(final String path) {
        LOGGER.info("to-be-implemented: isPlaceholder: " + path);
        return true;
    }
}
