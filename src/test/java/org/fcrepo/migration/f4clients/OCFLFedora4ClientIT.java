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

package org.fcrepo.migration.f4clients;

import org.apache.commons.io.FileUtils;
import org.fcrepo.migration.Fedora4Client;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Collection;
import java.util.UUID;

import static org.apache.commons.io.filefilter.FileFilterUtils.trueFileFilter;
import static org.fcrepo.migration.f4clients.OCFLFedora4Client.ObjectIdMapperType.FLAT;
import static org.fcrepo.migration.f4clients.OCFLFedora4Client.ObjectIdMapperType.PAIRTREE;
import static org.fcrepo.migration.f4clients.OCFLFedora4Client.ObjectIdMapperType.TRUNCATED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A basic suite of integration tests to test certain interaction patterns (and code) against the an OCFL version of
 * Fedora.
 *
 * @author awoods, copied from org.fcrepo.migration.f4clients.OCFLGoLangFedora4ClientIT.java by Remi Malessa
 * @since 4.4.1-SNAPSHOT
 */
public class OCFLFedora4ClientIT {

    private Fedora4Client client;
    private File storage;
    private File staging;

    @Before
    public void setup() throws BeansException {
        // Create directories expected in this test (based on `spring/ocfl-it-setup.xml`)
        storage = new File("target/test/ocfl/storage");
        staging = new File("target/test/ocfl/staging");

        storage.mkdirs();
        staging.mkdirs();

        final ConfigurableApplicationContext context = new ClassPathXmlApplicationContext("spring/ocfl-it-setup.xml");
        client = (Fedora4Client) context.getBean("fedora4Client");
    }

    /**
     * Testing integration of the exist() method.
     *
     * @author awoods, copied from org.fcrepo.migration.f4clients.OCFLGoLangFedora4ClientIT.java by Remi Malessa
     * @since 4.4.1-SNAPSHOT
     */
    @Test
    public void testExists() {

        final String id = UUID.randomUUID().toString();
        assertFalse("Object should not exist: " + id, client.exists(id));

        client.createResource(id);
        // Still should not exist... in the OCFL storage root
        assertFalse("Object should not exist: " + id, client.exists(id));
        client.createVersionSnapshot(id, "v1");
        assertTrue("Object should exist: " + id, client.exists(id));
    }

    /**
     * Testing integration of the createPlaceholder() method.
     * 
     * TODO Futher integration tests may be needed.
     * At time of writing, no methods exist to integrate with
     *
     * @author Dan Field
     * @since 4.4.1-SNAPSHOT
     */
    @Test
    public void testCreatePlaceholder() {

        final String newFileName = UUID.randomUUID().toString();
        boolean caught = false;
        try {
            final String returnedPath = client.createPlaceholder(newFileName);
        } catch (RuntimeException e) {
            caught = true;
        }
        assertFalse("createPlaceholder threw an exception", caught);
    }

    @Test
    public void testCreateResource() {

        final String id = UUID.randomUUID().toString();
        client.createResource(id);
    }

    @Test
    public void testCreateOrUpdateNonRDFResource() {
        final String path = UUID.randomUUID().toString();
        client.createResource(path);
        client.createOrUpdateNonRDFResource(path + "/v1/content/file.xml",
                new ByteArrayInputStream("<sample>test-1</sample>".getBytes()), "text/xml");
        client.createOrUpdateNonRDFResource(path + "/v1/content/file.xml",
                new ByteArrayInputStream("<sample>test-2</sample>".getBytes()), "text/xml");
        // TODO - check the content of the file?
    }

    @Test
    public void testFlatLayout() {
        client = new OCFLFedora4Client(storage.getPath(), staging.getPath(), FLAT);

        final String id = "flat-" + UUID.randomUUID().toString();
        client.createResource(id);

        client.createVersionSnapshot(id, "not-used");
        assertTrue("Object should exist: " + id, client.exists(id));
        assertTrue("Should exist in storage: " + id, new File(storage, id).exists());
    }

    @Test
    public void testPairtreeLayout() {
        final String storage2 = storage.getPath() + "-pair";
        client = new OCFLFedora4Client(storage2, staging.getPath(), PAIRTREE);

        final String id = "pairtree-" + UUID.randomUUID().toString();
        client.createResource(id);

        client.createVersionSnapshot(id, "not-used");
        assertTrue("Object should exist: " + id, client.exists(id));

        final Collection<File> files = FileUtils.listFilesAndDirs(
                new File(storage2),
                trueFileFilter(),
                trueFileFilter());

        File found = null;
        for (File f : files) {
            if (f.getName().equals(id.substring(id.length() - 4))) {
                found = f;
            }
        }

        assertNotNull(found);
        assertEquals(id.substring(id.length() - 4), found.getName());
        assertTrue("Should be more than half as many path elements as length of ID",
                id.length() / 2 < found.toPath().getNameCount());
    }

    @Test
    public void testTruncatedLayout() {
        final String storage2 = storage.getPath() + "-truncated";
        client = new OCFLFedora4Client(storage2, staging.getPath(), TRUNCATED);

        final String id = "truncated-" + UUID.randomUUID().toString();
        client.createResource(id);

        client.createVersionSnapshot(id, "not-used");
        assertTrue("Object should exist: " + id, client.exists(id));

        final Collection<File> files = FileUtils.listFilesAndDirs(
                new File(storage2),
                trueFileFilter(),
                trueFileFilter());

        File found = null;
        for (File f : files) {
            if (f.getName().length() > 40) {
                found = f;
            }
        }

        assertNotNull(found);

        // The "truncated" algorithm is three levels deep, with the fourth level being the full hash
        // ..each of the three levels contains sets of three characters from the hash.
        final String topLevelName = found.getParentFile().getParentFile().getParentFile().getName();
        assertEquals(topLevelName, found.getName().substring(0, 3));
    }
}
