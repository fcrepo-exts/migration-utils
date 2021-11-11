/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.pidlist;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

    @Before
    public void setUp() {
        // Test PIDs
        pidList = Arrays.asList("pid:1", "pid:2", "pid:3", "pid:4");

        System.out.println("test dir: " + testDir);

        // Define directory in which to find resume-file.
        manager = new ResumePidListManager(new File(testDir), false);
    }

    @After
    public void tearDown() {
        manager.reset();
    }

    @Test
    public void accept() {
        pidList.forEach(pid -> Assert.assertTrue(pid + " should be accepted", manager.accept(pid)));
    }

    @Test
    public void acceptIncrementalRuns() {
        Assert.assertTrue("pid:1 should be accepted", manager.accept("pid:1"));
        Assert.assertTrue("pid:2 should be accepted", manager.accept("pid:2"));

        // Simulate stopping the migration process... and start over
        manager = new ResumePidListManager(new File(testDir), false);
        Assert.assertFalse("pid:1 should NOT be accepted", manager.accept("pid:1"));
        Assert.assertFalse("pid:2 should NOT be accepted", manager.accept("pid:2"));

        // ..however, unprocessed PIDs should be "accepted"
        Assert.assertTrue("pid:3 should be accepted", manager.accept("pid:3"));
        Assert.assertTrue("pid:4 should be accepted", manager.accept("pid:4"));

        // Starting over again... no PIDs should be accepted
        manager = new ResumePidListManager(new File(testDir), false);
        pidList.forEach(pid -> Assert.assertFalse(pid + " should NOT be accepted", manager.accept(pid)));
    }

    @Test
    public void acceptAll() {
        Assert.assertTrue("pid:1 should be accepted", manager.accept("pid:1"));
        Assert.assertTrue("pid:2 should be accepted", manager.accept("pid:2"));

        // Simulate stopping the migration process... and start over - but accept all
        manager = new ResumePidListManager(new File(testDir), true);
        Assert.assertTrue("pid:1 should be accepted", manager.accept("pid:1"));
        Assert.assertTrue("pid:2 should be accepted", manager.accept("pid:2"));

        // ..however, unprocessed PIDs should be "accepted" - accept all
        Assert.assertTrue("pid:3 should be accepted", manager.accept("pid:3"));
        Assert.assertTrue("pid:4 should be accepted", manager.accept("pid:4"));

        // Starting over again... no PIDs should be accepted - but, accept all
        manager = new ResumePidListManager(new File(testDir), true);
        pidList.forEach(pid -> Assert.assertTrue(pid + " should be accepted", manager.accept(pid)));
    }
}