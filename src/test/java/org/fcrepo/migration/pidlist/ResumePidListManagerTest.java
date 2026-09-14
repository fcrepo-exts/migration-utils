/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.pidlist;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Unit test class for ResumePidListManager
 *
 * @author awoods
 * @since 2019-11-08
 */
public class ResumePidListManagerTest {

    private ResumePidListManager manager;
    private final String testDir = System.getProperty("test.output.dir");

    private List<String> pidList;

    @BeforeEach
    public void setUp() {
        // Test PIDs
        pidList = Arrays.asList("pid:1", "pid:2", "pid:3", "pid:4");

        System.out.println("test dir: " + testDir);

        // Define directory in which to find resume-file.
        manager = new ResumePidListManager(new File(testDir), false);
    }

    @AfterEach
    public void tearDown() {
        manager.reset();
    }

    @Test
    public void accept() {
        pidList.forEach(pid -> Assertions.assertTrue(manager.accept(pid), pid + " should be accepted"));
    }

    @Test
    public void acceptIncrementalRuns() {
        Assertions.assertTrue(manager.accept("pid:1"), "pid:1 should be accepted");
        Assertions.assertTrue(manager.accept("pid:2"), "pid:2 should be accepted");

        // Simulate stopping the migration process... and start over
        manager = new ResumePidListManager(new File(testDir), false);
        Assertions.assertFalse(manager.accept("pid:1"), "pid:1 should NOT be accepted");
        Assertions.assertFalse(manager.accept("pid:2"), "pid:2 should NOT be accepted");

        // ..however, unprocessed PIDs should be "accepted"
        Assertions.assertTrue(manager.accept("pid:3"), "pid:3 should be accepted");
        Assertions.assertTrue(manager.accept("pid:4"), "pid:4 should be accepted");

        // Starting over again... no PIDs should be accepted
        manager = new ResumePidListManager(new File(testDir), false);
        pidList.forEach(pid -> Assertions.assertFalse(manager.accept(pid), pid + " should NOT be accepted"));
    }

    @Test
    public void acceptAll() {
        Assertions.assertTrue(manager.accept("pid:1"), "pid:1 should be accepted");
        Assertions.assertTrue(manager.accept("pid:2"), "pid:2 should be accepted");

        // Simulate stopping the migration process... and start over - but accept all
        manager = new ResumePidListManager(new File(testDir), true);
        Assertions.assertTrue(manager.accept("pid:1"), "pid:1 should be accepted");
        Assertions.assertTrue(manager.accept("pid:2"), "pid:2 should be accepted");

        // ..however, unprocessed PIDs should be "accepted" - accept all
        Assertions.assertTrue(manager.accept("pid:3"), "pid:3 should be accepted");
        Assertions.assertTrue(manager.accept("pid:4"), "pid:4 should be accepted");

        // Starting over again... no PIDs should be accepted - but, accept all
        manager = new ResumePidListManager(new File(testDir), true);
        pidList.forEach(pid -> Assertions.assertTrue(manager.accept(pid), pid + " should be accepted"));
    }
}