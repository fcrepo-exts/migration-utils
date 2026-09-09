/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.migration;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Verifies that the version reported by the CLI is derived from the jar manifest.
 *
 * @author Dan Field
 */
public class MigrationVersionProviderTest {

    @Test
    public void shouldReportTheImplementationVersionFromTheManifest() {
        final var expected = MigrationVersionProvider.class.getPackage().getImplementationVersion();
        final var version = new MigrationVersionProvider().getVersion();

        assertEquals(1, version.length);
        if (expected == null) {
            // The tests run against target/classes, where there is no manifest to read.
            assertEquals("Migration Utils - " + MigrationVersionProvider.UNKNOWN_VERSION, version[0]);
        } else {
            assertEquals("Migration Utils - " + expected, version[0]);
        }
    }
}
