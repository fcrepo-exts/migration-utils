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

import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.User;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.layout.config.DefaultLayoutConfig;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import org.fcrepo.migration.ObjectIdMapperType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Misuses Fedora4client to write OCFL objects.
 * <p>
 * Exploits knowledge of how OCFL-based f4 clients work internally in order to
 * write OCFL objects. It would be better to use an OCFL client directly.
 * </p>
 *
 * @author apb@jhu.edu
 */
public class DefaultOcflDriver implements OcflDriver {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultOcflDriver.class);

    private final OcflRepository ocflRepo;
    private final Path storageRoot;
    private final Path stagingRoot;
    private final CommitInfo commitInfo;

    /**
     * Construct DefaultOcflDriver
     *
     * @param storage location of OCFL storage root
     * @param staging location of OCFL work directory
     * @param mapper how to map object ids to paths
     * @param commitUser the user to associate versions to
     */
    public DefaultOcflDriver(final String storage, final String staging,
                             final ObjectIdMapperType mapper,
                             final String commitUser) {
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

        this.commitInfo = commitInfo(commitUser);

        try {
            storageRoot = Files.createDirectories(Paths.get(storage));
            stagingRoot = Files.createDirectories(Paths.get(staging));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        LOGGER.debug("OCFL storage <{}>; staging <{}>", storageRoot, stagingRoot);
        ocflRepo = new OcflRepositoryBuilder()
                .layoutConfig(layoutConfig)
                .storage(FileSystemOcflStorage.builder().repositoryRoot(storageRoot).build())
                .workDir(stagingRoot)
                .build();

    }

    @Override
    public OcflSession open(final String id) {
        final String encodedId = URLEncoder.encode(id, StandardCharsets.UTF_8);
        return new OcflSession() {

            @Override
            public void put(final String path, final InputStream content) {
                LOGGER.debug("put file <{}> in object <{}>", path, id);

                final var destination = stagingRoot.resolve(encodedId).resolve(path);

                try {
                    Files.createDirectories(destination.getParent());
                    Files.copy(content, destination);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public void commit() {
                LOGGER.debug("commit object <{}>", id);

                ocflRepo.updateObject(ObjectVersionId.head(id), commitInfo, updater -> {
                    updater.addPath(stagingRoot.resolve(encodedId),
                            OcflOption.MOVE_SOURCE, OcflOption.OVERWRITE);
                });
            }
        };
    }

    @Override
    public void close() {
        ocflRepo.close();
    }

    private CommitInfo commitInfo(final String name) {
        return new CommitInfo().setMessage("Generated by Fedora 3 to Fedora 6 migration")
                .setUser(new User().setName(name));
    }

}
