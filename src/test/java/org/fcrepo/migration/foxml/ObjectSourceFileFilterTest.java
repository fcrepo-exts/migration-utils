/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.migration.foxml;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Covers the {@code setFileFilter} configuration hooks of the directory-based object sources.
 *
 * @author Dan Field
 */
public class ObjectSourceFileFilterTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private static final RegexFileFilter FILTER = new RegexFileFilter(Pattern.compile(".*"));

    @Test
    public void nativeSourceAcceptsFileFilter() throws IOException {
        final var source =
                new NativeFoxmlDirectoryObjectSource(tempDir.newFolder("native"), null, "localhost:8080");
        source.setFileFilter(FILTER);
        assertNotNull(source);
    }

    @Test
    public void exportedSourceAcceptsFileFilter() throws IOException {
        final var source =
                new ArchiveExportedFoxmlDirectoryObjectSource(tempDir.newFolder("exported"), "localhost:8080");
        source.setFileFilter(FILTER);
        assertNotNull(source);
    }
}
