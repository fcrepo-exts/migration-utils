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

package org.fcrepo.migration.pidlist;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.fcrepo.migration.Migrator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.FileFilter;
import java.util.Collections;

/**
 * A basic suite of integration tests to test certain interaction patterns (and code) against the an OCFL version of
 * Fedora.
 * ..using the "ResumePidListManager"
 *
 * @author awoods
 * @since 2019-11-11
 */
public class ResumePidListManagerIT {

    private ConfigurableApplicationContext context;
    private Migrator migrator;
    private File storage;
    private File staging;
    private File pidDir;

    private FileFilter fileFilter;

    @Before
    public void setup() throws Exception {
        // Create directories expected in this test (based on `spring/ocfl-pid-it-setup.xml`)
        storage = new File("target/test/ocfl/pid-it/storage");
        staging = new File("target/test/ocfl/pid-it/staging");
        pidDir = new File("target/test/ocfl/pid-it/pid");

        if (storage.exists()) {
            FileUtils.forceDelete(storage);
        }
        if (staging.exists()) {
            FileUtils.forceDelete(staging);
        }
        if (pidDir.exists()) {
            FileUtils.forceDelete(pidDir);
        }

        storage.mkdirs();
        staging.mkdirs();
        pidDir.mkdirs();

        context = new ClassPathXmlApplicationContext("spring/ocfl-pid-it-setup.xml");
        migrator = (Migrator) context.getBean("migrator");
        fileFilter = new RegexFileFilter("info%3afedora%2fexample.*");
    }

    @After
    public void tearDown() {
        context.close();
    }

    @Test
    public void testMigrate() throws Exception {
        final boolean acceptAll = false;
        final ResumePidListManager manager = new ResumePidListManager(pidDir, acceptAll);

        // There are three test OCFL objects: example%3a1, example%3a2, example%3a3
        migrator.setPidListManagers(Collections.singletonList(manager));
        migrator.run();

        final int numObjects = storage.listFiles(fileFilter).length;
        Assert.assertEquals(3, numObjects);
    }

    @Test
    public void testMigrateIncremental() throws Exception {
        final boolean acceptAll = false;

        // There are three test OCFL objects: example%3a1, example%3a2, example%3a3
        migrator.setPidListManagers(Collections.singletonList(new ResumePidListManager(pidDir, acceptAll)));
        // Only migrate 2 of 3 objects
        migrator.setLimit(2);
        migrator.run();
        context.close();

        int numObjects = storage.listFiles(fileFilter).length;
        Assert.assertEquals(2, numObjects);

        // Remove the previously exported objects, and resume the migration
        FileUtils.forceDelete(storage);
        FileUtils.forceDelete(staging);
        storage.mkdirs();
        staging.mkdirs();

        context = new ClassPathXmlApplicationContext("spring/ocfl-pid-it-setup.xml");
        migrator = (Migrator) context.getBean("migrator");
        migrator.setPidListManagers(Collections.singletonList(new ResumePidListManager(pidDir, acceptAll)));
        migrator.setLimit(-1); // migrate all
        migrator.run();

        numObjects = storage.listFiles(fileFilter).length;
        Assert.assertEquals(1, numObjects);
    }

}
