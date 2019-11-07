package org.fcrepo.migration.f4clients;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.fcrepo.migration.Fedora4Client;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author awoods
 * @since 2019-07-18
 */
public class OCFLGoLangFedora4Client implements Fedora4Client {

    private static final Logger LOGGER = getLogger(OCFLGoLangFedora4Client.class);

    private final File storageRoot;
    private final File stagingRoot;

    /**
     * Constructor
     * 
     * @param storage Root for OCFL Objects
     * @param staging directory for in-progress OCFL Objects
     */
    public OCFLGoLangFedora4Client(final String storage, final String staging) {

        // Ensure Storage Root exists
        this.storageRoot = new File(storage);
        if (!this.storageRoot.exists()) {
            ocflExecute("ocfl mkroot " + storageRoot.getAbsolutePath());

        } else if (!storageRoot.isDirectory()) {
            throw new RuntimeException("Storage directory must be a directory: " + storageRoot);
        }

        // Ensure Staging location exists
        this.stagingRoot = new File(staging);
        if (!stagingRoot.exists() && !stagingRoot.mkdirs()) {
            throw new RuntimeException("Unable to create staging directory: " + stagingRoot);
        }
    }

    /**
     * This method returns true if the resource exists in the storage root
     *
     * @param path to the resource
     * @return true if resource exists in 'storage' root
     */
    @Override
    public boolean exists(final String path) {
        final String output = ocflExecute("ocfl -r " + storageRoot.getAbsolutePath() + " ls -t " + path);

        final boolean exists = !StringUtils.isBlank(output);
        LOGGER.info("exists?: " + path + ", " + exists);
        return exists;
    }

    @Override
    public void createResource(final String path) {
        LOGGER.info("createResource: {}", path);

        final String ocflObject = objFromPath(path);

        // Ensure object exists in staging
        final File stagingObj = new File(stagingRoot, ocflObject);
        if (!stagingObj.exists() && !stagingObj.mkdirs()) {
            throw new RuntimeException("Unable to create staging object: " + stagingObj);
        }
    }

    @Override
    public String getRepositoryUrl() {
        LOGGER.info("to-be-implemented: getRepositoryUrl");
        return null;
    }

    @Override
    public void createOrUpdateRedirectNonRDFResource(final String path, final String url) {
        LOGGER.info("to-be-implemented: createOrUpdateRedirectNonRDFResource: " + path + ", " + url);

    }

    @Override
    public void createOrUpdateNonRDFResource(final String path, final InputStream content, final String contentType) {
        LOGGER.info("createOrUpdateNonRDFResource: " + path + ", " + contentType);

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

        // Copy provided content into staging file... in append-mode.
        try (final FileOutputStream outputStream = new FileOutputStream(stagingFile, true)) {
            try {
                IOUtils.copy(content, outputStream);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String filenameFromPath(final String path) {
        // return path element after final '/', or full path if no '/'
        return path.contains("/") ? path.substring(path.lastIndexOf('/') + 1) : path;
    }

    private String objFromPath(final String path) {
        // return path before final '/', or full path
        return path.contains("/") ? path.substring(0, path.lastIndexOf('/')) : path;
    }

    @Override
    public void createVersionSnapshot(final String path, final String versionId) {
        LOGGER.info("createVersionSnapshot: {}, version-id: {}", path, versionId);

        // Copy the staged object into the storage root, as a new OCFL version
        final String objPath = objFromPath(path);
        final File stagingObject = new File(stagingRoot, objPath);
        ocflExecute("ocfl -r " + storageRoot + " cp -r " + stagingObject + " " + objPath);

        // Remove the staged object
        try {
            FileUtils.deleteDirectory(stagingObject);
        } catch (IOException e) {
            LOGGER.warn("Unable to delete staged object: {}, {}", stagingObject, e.getMessage());
        }
    }

    @Override
    public void updateResourceProperties(final String path, final String sparqlUpdate) {
        LOGGER.info("to-be-implemented: updateResourceProperties: " + path + ", " + sparqlUpdate);

    }

    @Override
    public void updateNonRDFResourceProperties(final String path, final String sparqlUpdate) {
        LOGGER.info("to-be-implemented: updateNonRDFResourceProperties: " + path + ", " + sparqlUpdate);

    }

    /**
     * This method creates or returns an existing object in the staging-location
     */
    @Override
    public String createPlaceholder(final String path) {
        LOGGER.info("createPlaceholder: {}", path);

        final File placeholder =
                new File(stagingRoot,
                         path == null ? UUID.randomUUID().toString() : path);
        if (!placeholder.exists() && !placeholder.mkdirs()) {
            throw new RuntimeException("Unable to create directory: " + storageRoot.getAbsolutePath() + "/" + path);
        }
        return placeholder.getAbsolutePath();
    }

    @Override
    public String createNonRDFPlaceholder(final String path) {
        LOGGER.info("to-be-implemented: createNonRDFPlaceholder: " + path);
        return null;
    }

    @Override
    public boolean isPlaceholder(final String path) {
        LOGGER.info("to-be-implemented: isPlaceholder: " + path);
        return true;
    }

    /**
     * This method executes the backend OCFL commands
     *
     * @param command to execute
     * @return String response from execution
     */
    private String ocflExecute(final String command) {
        LOGGER.info("OCFL command: {}", command);

        final StringBuilder stdout = new StringBuilder();
        try {
            // Execute command
            final Process p = Runtime.getRuntime().exec(command);

            final BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            final BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

            // Read the standard out from the command
            String out;
            while ((out = stdInput.readLine()) != null) {
                stdout.append(out + "\n");
            }
            LOGGER.debug("standard output: '{}'", stdout);

            // Read any errors from the attempted command
            final StringBuilder stderr = new StringBuilder();
            String err;
            while ((err = stdError.readLine()) != null) {
                stderr.append(err + "\n");
            }
            LOGGER.debug("standard error: '{}'", stderr);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return stdout.toString();
    }
}
