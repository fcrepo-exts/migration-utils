/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.foxml;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Exercises the depth-first traversal of {@link FoxmlDirectoryDFSIterator} over a real
 * directory tree.
 *
 * @author Claude
 */
public class FoxmlDirectoryDFSIteratorTreeTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private static final Pattern MATCH_NONE = Pattern.compile("match-nothing-xyz");

    @Test
    public void descendsDirectoriesWhenFilterRejectsEverything() throws IOException {
        final File root = tempDir.newFolder("root");
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
        final File root = tempDir.newFolder("empty");
        final var iterator =
                new FoxmlDirectoryDFSIterator(root, null, null, new RegexFileFilter(Pattern.compile(".*")));
        assertThrows(IllegalStateException.class, iterator::next);
    }

    @Test
    public void removeIsUnsupported() throws IOException {
        final File root = tempDir.newFolder("root");
        final var iterator =
                new FoxmlDirectoryDFSIterator(root, null, null, new RegexFileFilter(Pattern.compile(".*")));
        assertThrows(UnsupportedOperationException.class, iterator::remove);
    }
}
