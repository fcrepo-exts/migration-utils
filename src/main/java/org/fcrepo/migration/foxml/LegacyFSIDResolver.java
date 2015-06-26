package org.fcrepo.migration.foxml;

import java.io.File;
import java.io.IOException;

/**
 * An extension of DirectoryScanningIDResolver for datastream directories of fedora
 * repositories using the legacy-fs storage implementation.
 *
 * @author mdurbin
 */
public class LegacyFSIDResolver extends DirectoryScanningIDResolver {

    /**
     * Basic constructor.
     * @param indexDir A directory that will serve as a lucene index directory to cache ID resolution.
     * @param dsRoot the root directory of the AkubraFS datastream store.
     * @throws IOException
     */
    public LegacyFSIDResolver(final File indexDir, final File dsRoot) throws IOException {
        super(indexDir, dsRoot);
    }

    /**
     * Basic constructor.
     * @param dsRoot the root directory of the AkubraFS datastream store.
     * @throws IOException
     */
    public LegacyFSIDResolver(final File dsRoot) throws IOException {
        super(null, dsRoot);
    }

    @Override
    protected String getInternalIdForFile(final File f) {
        return f.getName().replaceFirst("_", ":");
    }
}
