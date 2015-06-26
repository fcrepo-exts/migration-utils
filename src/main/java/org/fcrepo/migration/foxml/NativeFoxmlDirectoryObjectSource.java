package org.fcrepo.migration.foxml;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

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

    @Override
    public Iterator<FedoraObjectProcessor> iterator() {
        return new FoxmlDirectoryDFSIterator(root, resolver, fetcher, localFedoraServer);
    }

}
