/*
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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;


/**
 * Unit test class for UserProvidedPidListManager
 *
 * @author awoods
 * @since 2019-11-08
 */
public class UserProvidedPidListManagerTest {

    private UserProvidedPidListManager manager;

    private List<String> pidList;

    private File pidListFile;

    @Before
    public void setUp() throws Exception {
        // Test PIDs
        pidList = Arrays.asList("pid:1", "pid:2", "pid:3", "pid:4");

        // Create file in which to place test PIDs
        final String testDir = System.getProperty("test.output.dir");
        final StringBuilder tmpList = new StringBuilder();
        pidList.forEach(pid -> tmpList.append(pid).append("\n"));

        pidListFile = new File(testDir, "pid-list.txt");

        final BufferedWriter writer = new BufferedWriter(new FileWriter(pidListFile));
        writer.write(tmpList.toString());
        writer.close();

        // Class under test
        manager = new UserProvidedPidListManager(pidListFile);
    }

    @Test
    public void accept() {
        pidList.forEach(pid -> Assert.assertTrue(pid + " should be accepted", manager.accept(pid)));
    }

    @Test
    public void acceptAll() {
        manager = new UserProvidedPidListManager(null);
        pidList.forEach(pid -> Assert.assertTrue(pid + " should be accepted", manager.accept(pid)));
    }


    @Test
    public void acceptNotFound() {
        Assert.assertFalse("'bad' should NOT be accepted", manager.accept("bad"));
        Assert.assertFalse("'junk' should NOT be accepted", manager.accept("junk"));
    }
}