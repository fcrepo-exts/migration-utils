/*
 * Copyright 2015 DuraSpace, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
     * @throws IOException IO exception creating temp and index files/directories
     */
    public LegacyFSIDResolver(final File indexDir, final File dsRoot) throws IOException {
        super(indexDir, dsRoot);
    }

    /**
     * Basic constructor.
     * @param dsRoot the root directory of the AkubraFS datastream store.
     * @throws IOException IO exception creating temp and index files/directories
     */
    public LegacyFSIDResolver(final File dsRoot) throws IOException {
        super(null, dsRoot);
    }

    @Override
    protected String getInternalIdForFile(final File f) {
        return f.getName().replaceFirst("_", ":");
    }
}
