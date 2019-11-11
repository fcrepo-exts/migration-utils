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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.util.Collections;

/**
 * A basic suite of integration tests to test certain interaction patterns (and code) against the an OCFL version of
 * Fedora.
 * ..using the "UserProvidePidListManager"
 *
 * @author awoods
 * @since 2019-11-11
 */
public class UserProvidedPidListManagerIT {

    private ConfigurableApplicationContext context;
    private Migrator migrator;
    private File storage;
    private File staging;
    private File pidFile;

    @Before
    public void setup() throws Exception {
        // Create directories expected in this test (based on `spring/ocfl-user-it-setup.xml`)
        storage = new File("target/test/ocfl/user-it/storage");
        staging = new File("target/test/ocfl/user-it/staging");
        pidFile = new File("target/test/ocfl/user-it/pidlist.txt");

        if (storage.exists()) {
            FileUtils.forceDelete(storage);
        }
        if (staging.exists()) {
            FileUtils.forceDelete(staging);
        }
        if (pidFile.exists()) {
            FileUtils.forceDelete(pidFile);
        }

        storage.mkdirs();
        staging.mkdirs();

        context = new ClassPathXmlApplicationContext("spring/ocfl-user-it-setup.xml");
        migrator = (Migrator) context.getBean("migrator");
    }

    @After
    public void tearDown() {
        context.close();
    }

    @Test
    public void testMigrate() throws Exception {
        // Create PID-list file
        final BufferedWriter writer = new BufferedWriter(new FileWriter(pidFile));
        writer.write("example:3");
        writer.newLine();
        writer.write("example:2");
        writer.newLine();
        writer.write("example:1");
        writer.flush();

        final UserProvidedPidListManager manager = new UserProvidedPidListManager(pidFile);

        // There are three test OCFL objects: example%3a1, example%3a2, example%3a3
        migrator.setPidListManagers(Collections.singletonList(manager));
        migrator.run();

        final int numObjects = storage.listFiles((FileFilter) new RegexFileFilter("example.*")).length;
        Assert.assertEquals(3, numObjects) ;
    }

    @Test
    public void testMigrateIncremental() throws Exception {
        // Create PID-list file
        final BufferedWriter writer = new BufferedWriter(new FileWriter(pidFile));
        writer.write("example:3");
        writer.newLine();
        writer.write("example:1");
        writer.flush();

        // There are three test OCFL objects: example%3a1, example%3a2, example%3a3
        migrator.setPidListManagers(Collections.singletonList(new UserProvidedPidListManager(pidFile)));
        migrator.run();
        context.close();

        final int numObjects = storage.listFiles((FileFilter) new RegexFileFilter("example.*")).length;
        Assert.assertEquals(2, numObjects);
    }

}
