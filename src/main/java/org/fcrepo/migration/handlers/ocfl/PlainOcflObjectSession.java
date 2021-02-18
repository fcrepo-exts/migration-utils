/*
 * Copyright 2019 DuraSpace, Inc.
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

package org.fcrepo.migration.handlers.ocfl;

import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.OcflObjectUpdater;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.fcrepo.storage.ocfl.CommitType;
import org.fcrepo.storage.ocfl.InteractionModel;
import org.fcrepo.storage.ocfl.OcflObjectSession;
import org.fcrepo.storage.ocfl.OcflVersionInfo;
import org.fcrepo.storage.ocfl.PersistencePaths;
import org.fcrepo.storage.ocfl.ResourceContent;
import org.fcrepo.storage.ocfl.ResourceHeaders;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Barebones OcflObjectSession implementation that writes F3 resources to OCFL without F6 resource headers.
 * Operations other than writing are not supported.
 *
 * @author pwinckles
 */
public class PlainOcflObjectSession implements OcflObjectSession {

    private final MutableOcflRepository ocflRepo;
    private final String sessionId;
    private final String ocflObjectId;
    private final VersionInfo versionInfo;
    private final Path objectStaging;

    private final OcflOption[] ocflOptions;

    private final Set<String> deletePaths;

    private boolean closed = false;

    /**
     * @param sessionId the session's id
     * @param ocflRepo the OCFL client
     * @param ocflObjectId the OCFL object id
     * @param objectStaging the object's staging directory
     */
    public PlainOcflObjectSession(final String sessionId,
                                  final MutableOcflRepository ocflRepo,
                                  final String ocflObjectId,
                                  final Path objectStaging) {
        this.sessionId = sessionId;
        this.ocflRepo = ocflRepo;
        this.ocflObjectId = ocflObjectId;
        this.objectStaging = objectStaging;

        this.versionInfo = new VersionInfo();
        this.ocflOptions = new OcflOption[] {OcflOption.MOVE_SOURCE, OcflOption.OVERWRITE};
        this.deletePaths = new HashSet<>();
    }

    @Override
    public String sessionId() {
        return sessionId;
    }

    @Override
    public String ocflObjectId() {
        return ocflObjectId;
    }

    @Override
    public synchronized ResourceHeaders writeResource(final ResourceHeaders headers, final InputStream content) {
        enforceOpen();

        final var paths = resolvePersistencePaths(headers);

        final var contentPath = encode(paths.getContentFilePath());

        final var contentDst = createStagingPath(contentPath);
        write(content, contentDst);
        return headers;
    }

    @Override
    public synchronized void writeHeaders(final ResourceHeaders headers) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void versionCreationTimestamp(final OffsetDateTime timestamp) {
        versionInfo.setCreated(timestamp);
    }

    @Override
    public void versionAuthor(final String name, final String address) {
        versionInfo.setUser(name, address);
    }

    @Override
    public void versionMessage(final String message) {
        versionInfo.setMessage(message);
    }

    @Override
    public synchronized void commit() {
        enforceOpen();
        closed = true;

        if (Files.exists(objectStaging)) {
            ocflRepo.updateObject(ObjectVersionId.head(ocflObjectId), versionInfo, updater -> {
                if (Files.exists(objectStaging)) {
                    if (SystemUtils.IS_OS_WINDOWS) {
                        addDecodedPaths(updater, ocflOptions);
                    } else {
                        updater.addPath(objectStaging, ocflOptions);
                    }
                }
            });
        }

        // Because the of the way ArchiveGroupHandler works, a commit will only ever contains only adds or only deletes
        if (!deletePaths.isEmpty()) {
            ocflRepo.updateObject(ObjectVersionId.head(ocflObjectId), versionInfo, updater -> {
                deletePaths.forEach(updater::removeFile);
            });
        }

        cleanup();
    }

    @Override
    public void abort() {
        if (!closed) {
            closed = true;
            cleanup();
        }
    }

    @Override
    public void close() {
        abort();
    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException("Rollback is not supported");
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void deleteContentFile(final ResourceHeaders headers) {
        enforceOpen();

        final var paths = resolvePersistencePaths(headers);
        deletePaths.add(paths.getContentFilePath());
    }

    @Override
    public void deleteResource(final String resourceId) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResourceHeaders readHeaders(final String resourceId) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResourceHeaders readHeaders(final String resourceId, final String versionNumber) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResourceContent readContent(final String resourceId) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResourceContent readContent(final String resourceId, final String versionNumber) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<OcflVersionInfo> listVersions(final String resourceId) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Stream<ResourceHeaders> streamResourceHeaders() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void commitType(final CommitType commitType) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean containsResource(final String resourceId) {
        if (ocflRepo.containsObject(ocflObjectId)) {
            return true;
        } else {
            return false;
        }
    }

    private Path stagingPath(final String path) {
        return objectStaging.resolve(path);
    }

    private Path createStagingPath(final String path) {
        final var stagingPath = stagingPath(path);

        try {
            Files.createDirectories(stagingPath.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return stagingPath;
    }

    private void write(final InputStream content, final Path destination) {
        if (content != null) {
            try {
                Files.copy(content, destination, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private PersistencePaths resolvePersistencePaths(final ResourceHeaders headers) {
        final var resourceId = headers.getId();
        final PersistencePaths paths;

        if (InteractionModel.ACL.getUri().equals(headers.getInteractionModel())) {
            throw new UnsupportedOperationException("ACLs are not supported");
        } else if (InteractionModel.NON_RDF.getUri().equals(headers.getInteractionModel())) {
            paths = PersistencePaths.nonRdfResource(ocflObjectId, resourceId);
        } else if (headers.getInteractionModel() != null) {
            paths = PersistencePaths.rdfResource(ocflObjectId, resourceId);
        } else {
            throw new IllegalArgumentException(
                    String.format("Interaction model for resource %s must be populated.", resourceId));
        }

        return paths;
    }

    private String encode(final String value) {
        if (SystemUtils.IS_OS_WINDOWS) {
            final String encoded;
            if (value.contains("/")) {
                encoded = Arrays.stream(value.split("/"))
                        .map(s -> URLEncoder.encode(s, StandardCharsets.UTF_8))
                        .collect(Collectors.joining("/"));
            } else {
                encoded = URLEncoder.encode(value, StandardCharsets.UTF_8);
            }
            return  encoded;
        }
        return value;
    }

    private void addDecodedPaths(final OcflObjectUpdater updater, final OcflOption... ocflOptions) {
        try (var paths = Files.walk(objectStaging)) {
            paths.filter(Files::isRegularFile).forEach(file -> {
                final var logicalPath = windowsStagingPathToLogicalPath(file);
                updater.addPath(file, logicalPath, ocflOptions);
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String windowsStagingPathToLogicalPath(final Path path) {
        final var normalized = objectStaging.relativize(path).toString()
                .replace("\\", "/");
        return URLDecoder.decode(normalized, StandardCharsets.UTF_8);
    }

    private void cleanup() {
        if (Files.exists(objectStaging)) {
            FileUtils.deleteQuietly(objectStaging.toFile());
        }
    }

    private void enforceOpen() {
        if (closed) {
            throw new IllegalStateException(
                    String.format("Session %s is already closed!", sessionId));
        }
    }

}
