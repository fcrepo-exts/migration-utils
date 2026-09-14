/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.pidlist;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    @BeforeEach
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
        pidList.forEach(pid -> Assertions.assertTrue(manager.accept(pid), pid + " should be accepted"));
    }

    @Test
    public void acceptAll() {
        manager = new UserProvidedPidListManager(null);
        pidList.forEach(pid -> Assertions.assertTrue(manager.accept(pid), pid + " should be accepted"));
    }


    @Test
    public void acceptNotFound() {
        Assertions.assertFalse(manager.accept("bad"), "'bad' should NOT be accepted");
        Assertions.assertFalse(manager.accept("junk"), "'junk' should NOT be accepted");
    }

    @Test
    public void finishedProcessingAllPids() {
        Assertions.assertFalse(manager.finishedProcessingAllPids(), "not finished before processing");
        pidList.forEach(pid -> manager.accept(pid));
        Assertions.assertTrue(manager.finishedProcessingAllPids(), "finished once every pid is processed");
    }

    @Test
    public void acceptAllNeverFinishes() {
        final UserProvidedPidListManager acceptAll = new UserProvidedPidListManager(null);
        acceptAll.accept("pid:1");
        Assertions.assertFalse(acceptAll.finishedProcessingAllPids(), "accept-all mode has no completion state");
    }

    @Test
    public void missingFileThrows() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new UserProvidedPidListManager(new File("does-not-exist-98765.txt")));
    }
}