/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration;

import picocli.CommandLine.IVersionProvider;

/**
 * Supplies the version reported by the --version option, read from the
 * Implementation-Version attribute of the jar manifest so that it always
 * matches the version the artifact was built at.
 *
 * @author dfield
 */
public class MigrationVersionProvider implements IVersionProvider {

    /**
     * Reported when the manifest is unavailable, which is the case when running from
     * an exploded classes directory rather than from a packaged jar.
     */
    static final String UNKNOWN_VERSION = "development";

    @Override
    public String[] getVersion() {
        return new String[] {"Migration Utils - " + version()};
    }

    private String version() {
        final String version = MigrationVersionProvider.class.getPackage().getImplementationVersion();
        return version == null ? UNKNOWN_VERSION : version;
    }
}
