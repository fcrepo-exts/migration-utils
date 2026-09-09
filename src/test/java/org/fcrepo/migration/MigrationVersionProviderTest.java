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
    public void shouldReportTheVersionSuppliedByTheManifest() {
        assertEquals("Migration Utils - 7.0.0", MigrationVersionProvider.describe("7.0.0"));
    }

    @Test
    public void shouldFallBackWhenTheManifestSuppliesNoVersion() {
        // The manifest is absent when running from a classes directory rather than a packaged jar.
        assertEquals("Migration Utils - " + MigrationVersionProvider.UNKNOWN_VERSION,
                MigrationVersionProvider.describe(null));
    }

    @Test
    public void shouldReportASingleVersionLine() {
        final var version = new MigrationVersionProvider().getVersion();

        assertEquals(1, version.length);
        assertEquals(MigrationVersionProvider.describe(
                MigrationVersionProvider.class.getPackage().getImplementationVersion()), version[0]);
    }
}
