/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.migration;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Drives {@link PicocliMigrator#main(String[])} through failure paths to exercise the
 * command wiring, converter and execution exception handler.
 *
 * @author Dan Field
 */
public class PicocliMigratorTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private PrintStream originalErr;
    private ByteArrayOutputStream err;

    @Before
    public void captureErr() {
        originalErr = System.err;
        err = new ByteArrayOutputStream();
        System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
    }

    @After
    public void restoreErr() {
        System.setErr(originalErr);
    }

    private String errText() {
        return new String(err.toByteArray(), StandardCharsets.UTF_8);
    }

    @Test
    public void invalidAlgorithmIsHandledWithDebug() {
        PicocliMigrator.main(new String[] {
            "-t", "legacy",
            "-a", tempDir.getRoot().getAbsolutePath(),
            "--algorithm", "not-an-algorithm",
            "--debug"
        });

        assertTrue(errText().contains("Invalid algorithm"));
    }

    @Test
    public void invalidAlgorithmIsHandledWithoutDebug() {
        PicocliMigrator.main(new String[] {
            "-t", "legacy",
            "-a", tempDir.getRoot().getAbsolutePath(),
            "--algorithm", "not-an-algorithm"
        });

        assertTrue(errText().contains("Invalid algorithm"));
    }

    @Test
    public void unknownSourceTypeIsRejected() {
        PicocliMigrator.main(new String[] {
            "-t", "bogus",
            "-a", tempDir.getRoot().getAbsolutePath()
        });

        // The custom converter throws while parsing an unknown source type.
        assertTrue(errText().length() > 0);
    }
}
