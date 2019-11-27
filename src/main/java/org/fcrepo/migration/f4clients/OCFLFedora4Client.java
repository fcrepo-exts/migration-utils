
package org.fcrepo.migration.f4clients;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.User;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.layout.config.DefaultLayoutConfig;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.storage.FileSystemOcflStorage;
import org.apache.commons.io.IOUtils;
import org.fcrepo.migration.Fedora4Client;
import org.slf4j.Logger;


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
        FLAT, PAIRTREE, TRUNCATED
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

        LayoutConfig layoutConfig;
        switch (mapper) {
        case FLAT:
            layoutConfig = DefaultLayoutConfig.flatUrlConfig();
            break;
        case PAIRTREE:
            layoutConfig = DefaultLayoutConfig.pairTreeConfig();
            break;
        case TRUNCATED:
            layoutConfig = DefaultLayoutConfig.nTupleHashConfig();
            break;
        default:
            throw new RuntimeException("No implementation for the mapper: " + mapper);
        }

        // Create storage dir if does not exist
        final File storageDir = new File(storage);
        if (!storageDir.isDirectory()) {
            LOGGER.debug("Creating storage directory: {}", storageDir);
            storageDir.mkdirs();
        }

        // Create staging dir if does not exist
        final File stagingDir = new File(staging);
        if (!stagingDir.isDirectory()) {
            LOGGER.debug("Creating staging directory: {}", stagingDir);
            stagingDir.mkdirs();
        }

        LOGGER.debug("OCFLFedora4Client: {}, {}", storage, staging);
        final Path repoDir = Paths.get(this.storageRoot);
        ocflRepo = new OcflRepositoryBuilder()
                .layoutConfig(layoutConfig)
                .build(FileSystemOcflStorage.builder().build(repoDir), stagingDir.toPath());

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

    /**
     * Creates a new resource at the given path.
     *
     * @param path the path to the new resource
     */
    @Override
    public void createResource(final String path) {
        LOGGER.info("createResource: {}", path);

        final String ocflObject = objFromPath(path);
        final File stagingObj = new File(stagingRoot, ocflObject);
        if (!stagingObj.exists() && !stagingObj.mkdirs()) {
            throw new RuntimeException("Unable to create staging object: " + stagingObj);
        }
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
        LOGGER.debug("createOrUpdateNonRDFResource: {}, {}", path, contentType);

        final String ocflObject = objFromPath(path);
        final String ocflFilename = filenameFromPath(path);

        // Ensure object exists in staging
        final File stagingObj = new File(stagingRoot, ocflObject);
        if (!stagingObj.exists() && !stagingObj.mkdirs()) {
            throw new RuntimeException("Unable to create staging object: " + stagingObj);
        }

        // Ensure resource file exists in staging
        final File stagingFile = new File(stagingObj, ocflFilename);
        try {
            if (!stagingFile.createNewFile()) {
                // staging-file already existed
                LOGGER.debug("File already exists, {}", stagingFile);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Copy provided content into staging file
        try {
            final FileOutputStream outputStream = new FileOutputStream(stagingFile, false);
            try {
                IOUtils.copy(content, outputStream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a version snapshot for the resource (or graph) at the given path.
     *
     * @param path the path of the resource to be versioned
     * @param versionId a label for the version
     */
    @Override
    public void createVersionSnapshot(final String path, final String versionId) {
        LOGGER.info("createVersionSnapshot: {}, version-id: {}", path, versionId);

        // Copy the staged object into the storage root, as a new OCFL version
        final String ocflObject = objFromPath(path);

        final File stagingObject = new File(stagingRoot, ocflObject);
        final User user = new User().setName("name").setAddress("address");
        final CommitInfo defaultCommitInfo = new CommitInfo().setMessage("message").setUser(user);
        ocflRepo.putObject(ObjectVersionId.head(ocflObject), stagingObject.toPath(), defaultCommitInfo,
                OcflOption.MOVE_SOURCE);
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
        LOGGER.info("createPlaceholder: {} ", path);

        final String placeholderPath;
        // if no path requested, generate one
        if (path == null || path.isEmpty()) {
            // Use a UUID to avoid collisions with other files / tests
            placeholderPath = UUID.randomUUID().toString();
        } else {
            placeholderPath = path;
        }

        final File placeholder = new File(stagingRoot, placeholderPath);
        if (!placeholder.exists() && !placeholder.mkdirs()) {
            throw new RuntimeException("Unable to create directory: " + placeholder.getAbsolutePath());
        }
        LOGGER.info("createPlaceholder: {} ", placeholder.getAbsolutePath());
        return placeholder.getAbsolutePath();
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

    /**
     * Closes any resources the client has open
     */
    public void close() {
        ocflRepo.close();
    }

    /**
     * @param path the path to the new resource
     * @return object ID
     */
    private String objFromPath(final String path) {
        // return path before final '/', or full path
        return path.contains("/") ? path.substring(0, path.lastIndexOf('/')) : path;
    }

    /**
     * @param path the path to a resource
     * @return file name
     */
    private String filenameFromPath(final String path) {
        // return path element after final '/', or full path if no '/'
        return path.contains("/") ? path.substring(path.lastIndexOf('/') + 1) : path;
    }
}
