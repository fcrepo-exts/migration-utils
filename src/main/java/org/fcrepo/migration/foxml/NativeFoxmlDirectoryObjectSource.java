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
import java.io.FileFilter;
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.fcrepo.migration.FedoraObjectProcessor;
import org.fcrepo.migration.ObjectSource;

/**
 * An ObjectSource implementation that exposes FOXML from a provided directory.
 * @author mdurbin
 */

public class NativeFoxmlDirectoryObjectSource implements ObjectSource {

    private InternalIDResolver resolver;

    private URLFetcher fetcher;

    private File root;

    private String localFedoraServer;

    /**
     * Defaults to match any filename that doesn't begin with a "." character.
     */
    private FileFilter fileFilter = new RegexFileFilter(Pattern.compile("^[^\\.].*$"));

    /**
     * A constructor for use with the data storage directories that underly a
     * fedora 3.x repository.  First, this constructor will build an index of
     * all of the datastreams in the provided datastreamStore directory for use
     * resolving internal id references within the foxml.
     * @param objectStore a directory containing just directories and FOXML files
     * @param resolver an InternalIDResolver implementation that can resolve
     *                 references to internally managed datastreams.
     * @param localFedoraServer the domain and port for the server that hosted the fedora objects in the format
     *                          "localhost:8080".
     * @throws IOException
     */
    public NativeFoxmlDirectoryObjectSource(final File objectStore,
            final InternalIDResolver resolver, final String localFedoraServer) throws IOException {
        this.root = objectStore;
        this.resolver = resolver;
        this.fetcher = new HttpClientURLFetcher();
        this.localFedoraServer = localFedoraServer;
    }

    /**
     * set the fetcher.
     * @param fetcher the fetcher
     */
    public void setFetcher(final URLFetcher fetcher) {
        this.fetcher = fetcher;
    }

    /**
     * Sets a FileFilter to determine which files will be considered as object
     * files in the source directories.
     * @param fileFilter a FileFilter implementation
     */
    public void setFileFilter(final FileFilter fileFilter) {
        this.fileFilter = fileFilter;
    }

    @Override
    public Iterator<FedoraObjectProcessor> iterator() {
        return new FoxmlDirectoryDFSIterator(root, resolver, fetcher, localFedoraServer, fileFilter);
    }

}
