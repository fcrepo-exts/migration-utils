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
        return new String[] {describe(MigrationVersionProvider.class.getPackage().getImplementationVersion())};
    }

    /**
     * Formats the version for display, substituting a placeholder when the manifest did not
     * supply one. Separated from the manifest lookup so that both outcomes can be tested.
     *
     * @param implementationVersion the manifest's Implementation-Version, or null if unavailable
     * @return the string reported by the version option
     */
    static String describe(final String implementationVersion) {
        return "Migration Utils - " + (implementationVersion == null ? UNKNOWN_VERSION : implementationVersion);
    }
}
