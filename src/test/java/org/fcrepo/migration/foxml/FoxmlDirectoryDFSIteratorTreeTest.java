/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.migration.foxml;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises the depth-first traversal of {@link FoxmlDirectoryDFSIterator} over a real
 * directory tree.
 *
 * @author Dan Field
 */
public class FoxmlDirectoryDFSIteratorTreeTest {

    @TempDir
    public Path tempDir;

    private static final Pattern MATCH_NONE = Pattern.compile("match-nothing-xyz");

    @Test
    public void descendsDirectoriesWhenFilterRejectsEverything() throws IOException {
        final File root = Files.createDirectory(tempDir.resolve("root")).toFile();
        final File sub = new File(root, "sub");
        sub.mkdir();
        new File(sub, "child.txt").createNewFile();

        final var iterator =
                new FoxmlDirectoryDFSIterator(root, null, null, new RegexFileFilter(MATCH_NONE));

        // Walking the whole tree (descending into "sub" and popping back) yields no matches.
        assertFalse(iterator.hasNext());
    }

    @Test
    public void nextThrowsWhenExhausted() throws IOException {
        final File root = Files.createDirectory(tempDir.resolve("empty")).toFile();
        final var iterator =
                new FoxmlDirectoryDFSIterator(root, null, null, new RegexFileFilter(Pattern.compile(".*")));
        assertThrows(IllegalStateException.class, iterator::next);
    }

    @Test
    public void removeIsUnsupported() throws IOException {
        final File root = Files.createDirectory(tempDir.resolve("root")).toFile();
        final var iterator =
                new FoxmlDirectoryDFSIterator(root, null, null, new RegexFileFilter(Pattern.compile(".*")));
        assertThrows(UnsupportedOperationException.class, iterator::remove);
    }
}
