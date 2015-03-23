package org.fcrepo.migration.foxml11;

import org.fcrepo.migration.FedoraObjectProcessor;
import org.fcrepo.migration.ObjectSource;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

/**
 * An ObjectSource implementation that exposes FOXML from a provided directory.
 */

public class NativeFoxmlDirectoryObjectSource implements ObjectSource {

    private InternalIDResolver resolver;

    private URLFetcher fetcher;

    private File root;

    /**
     * A constructor for use with the data storage directories that underly a
     * fedora 3.x repository.  First, this constructor will build an index of
     * all of the datastreams in the provided datastreamStore directory for use
     * resolving internal id references within the foxml.
     * @param objectStore a directory containing just directories and FOXML files
     * @param resolver an InternalIDResolver implementation that can resolve 
     *                 references to internally managed datastreams.                 
     */
    public NativeFoxmlDirectoryObjectSource(final File objectStore, final InternalIDResolver resolver) throws IOException {
        this.root = objectStore;
        this.resolver = resolver;
        this.fetcher = new HttpClientURLFetcher();
    }

    public void setFetcher(final URLFetcher fetcher) {
        this.fetcher = fetcher;
    }

    @Override
    public Iterator<FedoraObjectProcessor> iterator() {
        return new FoxmlDirectoryDFSIterator(root, resolver, fetcher);
    }
    
}
