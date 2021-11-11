/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.foxml;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * An extension of DirectoryScanningIDResolver for datastream directories of fedora
 * repositories using the akubra-fs storage implementation.
 *
 * @author mdurbin
 */
public class AkubraFSIDResolver extends DirectoryScanningIDResolver {

    /**
     * Basic constructor.
     * @param indexDir A directory that will serve as a lucene index directory to cache ID resolution.
     * @param dsRoot the root directory of the AkubraFS datastream store.
     * @throws IOException IO exception creating temp and index files/directories
     */
    public AkubraFSIDResolver(final File indexDir, final File dsRoot) throws IOException {
        super(indexDir, dsRoot);
    }

    /**
     * Basic constructor.
     * @param dsRoot the root directory of the AkubraFS datastream store.
     * @throws IOException IO exception creating temp and index files/directories
     */
    public AkubraFSIDResolver(final File dsRoot) throws IOException {
        super(null, dsRoot);
    }

    @Override
    protected String getInternalIdForFile(final File f) {
        String id = f.getName();
        try {
            id = URLDecoder.decode(id, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        if (!id.startsWith("info:fedora/")) {
            throw new IllegalArgumentException(f.getName()
                    + " does not appear to be a valid akubraFS datastream file!");
        }
        id = id.substring("info:fedora/".length());
        id = id.replace('/', '+');
        return id;
    }
}
