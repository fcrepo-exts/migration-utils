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

package org.fcrepo.migration;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.benmanes.caffeine.cache.Caffeine;
import edu.wisc.library.ocfl.api.OcflConfig;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import edu.wisc.library.ocfl.core.path.mapper.LogicalPathMappers;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import org.apache.commons.lang3.SystemUtils;
import org.fcrepo.migration.handlers.ocfl.PlainOcflObjectSessionFactory;
import org.fcrepo.storage.ocfl.CommitType;
import org.fcrepo.storage.ocfl.DefaultOcflObjectSessionFactory;
import org.fcrepo.storage.ocfl.OcflObjectSessionFactory;
import org.fcrepo.storage.ocfl.cache.CaffeineCache;
import org.springframework.beans.factory.FactoryBean;

import java.nio.file.Path;
import java.time.Duration;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;

/**
 * Spring FactoryBean for easy OcflObjectSessionFactory creation.
 *
 * @author pwinckles
 */
public class OcflSessionFactoryFactoryBean implements FactoryBean<OcflObjectSessionFactory> {

    private final Path ocflRoot;
    private final Path stagingDir;
    private final MigrationType migrationType;
    private final String user;
    private final String userUri;
    private final DigestAlgorithm digestAlgorithm;
    private final boolean disableChecksumValidation;

    /**
     * @param ocflRoot OCFL storage root
     * @param stagingDir OCFL staging dir
     * @param migrationType migration type
     * @param user user to add to OCFL versions
     * @param userUri user's address
     * @param digestAlgorithm The digest algorithm to use.
     * @param disableChecksumValidation whether to verify fedora3 checksums or not
     */
    public OcflSessionFactoryFactoryBean(final Path ocflRoot,
                                         final Path stagingDir,
                                         final MigrationType migrationType,
                                         final String user,
                                         final String userUri,
                                         final DigestAlgorithm digestAlgorithm,
                                         final boolean disableChecksumValidation) {
        this.ocflRoot = ocflRoot;
        this.stagingDir = stagingDir;
        this.migrationType = migrationType;
        this.user = user;
        this.userUri = userUri;
        this.digestAlgorithm = digestAlgorithm;
        this.disableChecksumValidation = disableChecksumValidation;
    }

    /**
     * @param ocflRoot OCFL storage root
     * @param stagingDir OCFL staging dir
     * @param migrationType migration type
     * @param user user to add to OCFL versions
     * @param userUri user's address
     * @param disableChecksumValidation whether to verify fedora3 checksums or not
     */
    public OcflSessionFactoryFactoryBean(final Path ocflRoot,
                                         final Path stagingDir,
                                         final MigrationType migrationType,
                                         final String user,
                                         final String userUri,
                                         final boolean disableChecksumValidation) {
        this(ocflRoot, stagingDir, migrationType, user, userUri, DigestAlgorithm.sha512, disableChecksumValidation);
    }

    @Override
    public OcflObjectSessionFactory getObject() {
        final var logicalPathMapper = SystemUtils.IS_OS_WINDOWS ?
                LogicalPathMappers.percentEncodingWindowsMapper() : LogicalPathMappers.percentEncodingLinuxMapper();

        final var config = new OcflConfig();
        config.setDefaultDigestAlgorithm(this.digestAlgorithm);

        final var ocflRepo =  new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .logicalPathMapper(logicalPathMapper)
                .storage(FileSystemOcflStorage.builder().repositoryRoot(ocflRoot).build())
                .workDir(stagingDir)
                .ocflConfig(config)
                .buildMutable();

        if (migrationType == MigrationType.FEDORA_OCFL) {
            final var objectMapper = new ObjectMapper()
                    .configure(WRITE_DATES_AS_TIMESTAMPS, false)
                    .registerModule(new JavaTimeModule())
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);

            final var headersCache = Caffeine.newBuilder()
                    .maximumSize(512)
                    .expireAfterAccess(Duration.ofMinutes(10))
                    .build();

            final var rootIdCache = Caffeine.newBuilder()
                    .maximumSize(512)
                    .expireAfterAccess(Duration.ofMinutes(10))
                    .build();

            return new DefaultOcflObjectSessionFactory(ocflRepo, stagingDir, objectMapper,
                    new CaffeineCache<>(headersCache),
                    new CaffeineCache<>(rootIdCache),
                    CommitType.NEW_VERSION,
                    "Generated by Fedora 3 to Fedora 6 migration", user, userUri);
        } else {
            return new PlainOcflObjectSessionFactory(ocflRepo, stagingDir,
                    "Generated by Fedora 3 to Fedora 6 migration", user, userUri,
                    disableChecksumValidation);
        }
    }

    @Override
    public Class<?> getObjectType() {
        return OcflObjectSessionFactory.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

}
