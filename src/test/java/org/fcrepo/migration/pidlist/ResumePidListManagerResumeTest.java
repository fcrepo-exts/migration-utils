/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.migration.pidlist;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Covers the constructor validation and the resume-state mismatch handling of
 * {@link ResumePidListManager}.
 *
 * @author Dan Field
 */
public class ResumePidListManagerResumeTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test(expected = IllegalArgumentException.class)
    public void constructorRejectsNonDirectory() throws IOException {
        final File notADir = tempDir.newFile("not-a-dir.txt");
        new ResumePidListManager(notADir, false);
    }

    @Test(expected = IllegalStateException.class)
    public void mismatchedResumeStateThrows() throws IOException {
        final File pidDir = tempDir.newFolder("pids");

        // First run establishes a resume file at index 2 with value "pid:2".
        final ResumePidListManager first = new ResumePidListManager(pidDir, false);
        first.accept("pid:1");
        first.accept("pid:2");

        // Resuming and then diverging from the recorded ordering must fail.
        final ResumePidListManager resumed = new ResumePidListManager(pidDir, false);
        resumed.accept("pid:1");
        resumed.accept("divergent");
        resumed.accept("pid:3");
    }
}
