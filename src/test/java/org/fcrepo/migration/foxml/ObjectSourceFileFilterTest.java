/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.migration.foxml;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers the {@code setFileFilter} configuration hooks of the directory-based object sources.
 *
 * @author Dan Field
 */
public class ObjectSourceFileFilterTest {

    @TempDir
    public Path tempDir;

    private static final RegexFileFilter FILTER = new RegexFileFilter(Pattern.compile(".*"));

    @Test
    public void nativeSourceAcceptsFileFilter() throws IOException {
        final var source =
                new NativeFoxmlDirectoryObjectSource(
                        Files.createDirectory(tempDir.resolve("native")).toFile(), null, "localhost:8080");
        source.setFileFilter(FILTER);
        assertNotNull(source);
    }

    @Test
    public void exportedSourceAcceptsFileFilter() throws IOException {
        final var source =
                new ArchiveExportedFoxmlDirectoryObjectSource(
                        Files.createDirectory(tempDir.resolve("exported")).toFile(), "localhost:8080");
        source.setFileFilter(FILTER);
        assertNotNull(source);
    }
}
