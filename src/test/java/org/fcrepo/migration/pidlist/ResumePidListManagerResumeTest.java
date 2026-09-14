/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.migration.pidlist;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers the constructor validation and the resume-state mismatch handling of
 * {@link ResumePidListManager}.
 *
 * @author Dan Field
 */
public class ResumePidListManagerResumeTest {

    @TempDir
    public Path tempDir;

    @Test
    public void constructorRejectsNonDirectory() throws IOException {
        final File notADir = Files.createFile(tempDir.resolve("not-a-dir.txt")).toFile();
        assertThrows(IllegalArgumentException.class, () -> new ResumePidListManager(notADir, false));
    }

    @Test
    public void mismatchedResumeStateThrows() throws IOException {
        final File pidDir = Files.createDirectory(tempDir.resolve("pids")).toFile();

        // First run establishes a resume file at index 2 with value "pid:2".
        final ResumePidListManager first = new ResumePidListManager(pidDir, false);
        first.accept("pid:1");
        first.accept("pid:2");

        // Resuming and then diverging from the recorded ordering must fail.
        assertThrows(IllegalStateException.class, () -> {
            final ResumePidListManager resumed = new ResumePidListManager(pidDir, false);
            resumed.accept("pid:1");
            resumed.accept("divergent");
            resumed.accept("pid:3");
        });
    }
}
