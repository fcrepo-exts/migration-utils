/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.foxml;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Exercises the shared indexing and lookup behaviour of {@link DirectoryScanningIDResolver}
 * via a concrete subclass.
 *
 * @author Dan Field
 */
public class DirectoryScanningIDResolverTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    /**
     * Resolver that treats each file's name as its internal id.
     */
    private static class NameResolver extends DirectoryScanningIDResolver {
        NameResolver(final File indexDir, final File dsRoot) throws IOException {
            super(indexDir, dsRoot);
        }

        @Override
        protected String getInternalIdForFile(final File f) {
            return f.getName();
        }
    }

    /**
     * Resolver that maps every file to the same id, to force an ambiguous lookup.
     */
    private static class ConstantResolver extends DirectoryScanningIDResolver {
        ConstantResolver(final File indexDir, final File dsRoot) throws IOException {
            super(indexDir, dsRoot);
        }

        @Override
        protected String getInternalIdForFile(final File f) {
            return "dup";
        }
    }

    private File datastreamTree() throws IOException {
        final File dsRoot = tempDir.newFolder("datastreams");
        final File sub = new File(dsRoot, "sub");
        sub.mkdir();
        new File(sub, "fileA").createNewFile();
        new File(dsRoot, "fileB").createNewFile();
        return dsRoot;
    }

    @Test
    public void resolvesIndexedIdAndReportsMissing() throws IOException {
        // A null index directory triggers the temporary-index branch.
        final NameResolver resolver = new NameResolver(null, datastreamTree());
        try {
            assertNotNull(resolver.resolveInternalID("fileA"));
            assertThrows(RuntimeException.class, () -> resolver.resolveInternalID("does-not-exist"));
        } finally {
            resolver.close();
        }
    }

    @Test
    public void ambiguousIdThrows() throws IOException {
        final File indexDir = tempDir.newFolder("index-dup");
        final ConstantResolver resolver = new ConstantResolver(indexDir, datastreamTree());
        try {
            assertThrows(IllegalStateException.class, () -> resolver.resolveInternalID("dup"));
        } finally {
            resolver.close();
        }
    }

    @Test
    public void reusesExistingIndex() throws IOException {
        final File indexDir = tempDir.newFolder("index-cache");
        final File dsRoot = datastreamTree();

        final NameResolver first = new NameResolver(indexDir, dsRoot);
        first.close();

        // Second construction finds a populated index directory and reuses it.
        final NameResolver second = new NameResolver(indexDir, dsRoot);
        try {
            assertNotNull(second.resolveInternalID("fileB"));
        } finally {
            second.close();
        }
    }
}
