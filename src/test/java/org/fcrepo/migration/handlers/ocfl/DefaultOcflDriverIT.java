/**
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

package org.fcrepo.migration.handlers.ocfl;

import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedTruncatedNTupleConfig;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.UUID;

import static org.apache.commons.io.filefilter.FileFilterUtils.trueFileFilter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * A basic suite of integration tests to test certain interaction patterns (and code) against the an OCFL version of
 * Fedora.
 *
 * @author awoods, copied from org.fcrepo.migration.f4clients.OCFLGoLangFedora4ClientIT.java by Remi Malessa
 * @since 4.4.1-SNAPSHOT
 */
public class DefaultOcflDriverIT {

    private static final String USER = "fedoraAdmin";
    private static final String URI = "info:fedora/fedoraAdmin";

    private OcflDriver ocflDriver;
    private OcflRepository ocflRepo;
    private File storage;
    private File staging;

    @Before
    public void setup() throws IOException {
        // Create directories expected in this test (based on `spring/ocfl-it-setup.xml`)
        storage = new File("target/test/ocfl/storage");
        staging = new File("target/test/ocfl/staging");

        storage.mkdirs();
        staging.mkdirs();

        FileUtils.cleanDirectory(storage);
        FileUtils.cleanDirectory(staging);
    }

    @After
    public void after() {
        ocflDriver.close();
        ocflRepo.close();
        ocflDriver = null;
        ocflRepo = null;
    }

    @Test
    public void testCreateOrUpdateNonRDFResource() throws IOException {
        setupDriver();
        final String id = UUID.randomUUID().toString();
        final String path = "v1/content/file.xml";
        final var session = ocflDriver.open(id);

        session.put(path,
                new ByteArrayInputStream("<sample>test-1</sample>".getBytes()));
        session.commit();

        session.put(path,
                new ByteArrayInputStream("<sample>test-2</sample>".getBytes()));
        session.commit();

        final var objDesc = ocflRepo.describeObject(id);

        assertEquals(2, objDesc.getVersionMap().size());

        final var obj = ocflRepo.getObject(ObjectVersionId.head(id));
        assertEquals("<sample>test-2</sample>", IOUtils.toString(obj.getFile(path).getStream()));
    }

    @Test
    public void testTruncatedLayout() {
        setupDriver();

        final String id = "truncated-" + UUID.randomUUID().toString();
        final String hashedId = DigestUtils.sha256Hex(id);
        final var session = ocflDriver.open(id);

        session.put("test.txt", new ByteArrayInputStream("testing".getBytes()));
        session.commit();

        final Collection<File> files = FileUtils.listFilesAndDirs(
                storage,
                trueFileFilter(),
                trueFileFilter());

        File found = null;
        for (File f : files) {
            if (f.getName().length() > 40) {
                found = f;
            }
        }

        assertNotNull(found);
        assertEquals(hashedId, found.getName());
        // Object directory is 3 directories down from the storage root
        assertEquals(storage, found.getParentFile().getParentFile().getParentFile().getParentFile());
    }

    private void setupDriver() {
        ocflDriver = new DefaultOcflDriver(storage.toString(), staging.toString(), USER, URI);
        setupRepo();
    }

    private void setupRepo() {
        ocflRepo = new OcflRepositoryBuilder()
                .layoutConfig(new HashedTruncatedNTupleConfig())
                .storage(FileSystemOcflStorage.builder().repositoryRoot(storage.toPath()).build())
                .workDir(staging.toPath())
                .build();
    }

}
