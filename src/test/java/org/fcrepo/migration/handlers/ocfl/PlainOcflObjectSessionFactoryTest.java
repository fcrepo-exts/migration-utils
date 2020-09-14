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
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedTruncatedNTupleConfig;
import edu.wisc.library.ocfl.core.path.mapper.LogicalPathMappers;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import org.apache.commons.lang3.SystemUtils;
import org.fcrepo.storage.ocfl.OcflObjectSessionFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author pwinckles
 */
public class PlainOcflObjectSessionFactoryTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private Path ocflRoot;
    private Path staging;

    private MutableOcflRepository ocflRepo;
    private OcflObjectSessionFactory sessionFactory;

    @Before
    public void setup() throws IOException {
        ocflRoot = tempDir.newFolder("ocfl").toPath();
        staging = tempDir.newFolder("staging").toPath();

        final var logicalPathMapper = SystemUtils.IS_OS_WINDOWS ?
                LogicalPathMappers.percentEncodingWindowsMapper() : LogicalPathMappers.percentEncodingLinuxMapper();

        ocflRepo = new OcflRepositoryBuilder()
                .layoutConfig(new HashedTruncatedNTupleConfig())
                .logicalPathMapper(logicalPathMapper)
                .storage(FileSystemOcflStorage.builder().repositoryRoot(ocflRoot).build())
                .workDir(staging)
                .buildMutable();

        sessionFactory = new PlainOcflObjectSessionFactory(ocflRepo, staging,
                "testing", "fedoraAdmin", "info:fedora/fedoraAdmin");
    }

    @Test
    public void returnDifferentSessionsForTheSameObject() {
        final var session1 = sessionFactory.newSession("obj1");
        final var session2 = sessionFactory.newSession("obj1");

        assertNotEquals(session1.sessionId(), session2.sessionId());
    }

    @Test
    public void returnAnExistingOpenSession() {
        final var session1 = sessionFactory.newSession("obj1");
        final var session2 = sessionFactory.existingSession(session1.sessionId());

        assertTrue(session2.isPresent());
        assertEquals(session1.sessionId(), session2.get().sessionId());
    }

    @Test
    public void returnNothingWhenThereIsNotAnExistingOpenSession() {
        final var session1 = sessionFactory.newSession("obj1");

        session1.abort();

        final var session2 = sessionFactory.existingSession(session1.sessionId());

        assertTrue(session2.isEmpty());
    }

    @Test
    public void closeFactoryAndAllActiveSessions() {
        final var session1 = sessionFactory.newSession("obj1");
        final var session2 = sessionFactory.newSession("obj2");

        sessionFactory.close();

        assertFalse(session1.isOpen());
        assertFalse(session2.isOpen());
    }

}
