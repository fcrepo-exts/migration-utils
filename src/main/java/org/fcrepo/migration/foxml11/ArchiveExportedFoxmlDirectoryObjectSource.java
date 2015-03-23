package org.fcrepo.migration.foxml11;

import org.fcrepo.migration.FedoraObjectProcessor;
import org.fcrepo.migration.ObjectSource;

import java.io.File;
import java.util.Iterator;

/**
 * An ObjectSource implementation that exposes FOXML from a provided directory.
 * The FOXML is expected to have been produced using the export API method with
 * the "archive" context.  As such, each file will be a completely self-contained
 * serialization of the Fedora 3 object.
 */
public class ArchiveExportedFoxmlDirectoryObjectSource implements ObjectSource {
    
    private File root;
    
    private URLFetcher fetcher;
    
    public ArchiveExportedFoxmlDirectoryObjectSource(final File exportDir) {
        this.root = exportDir;
        this.fetcher = new HttpClientURLFetcher();
    }
    
    public void setFetcher(URLFetcher fetcher) {
        this.fetcher = fetcher;
    }
    
    @Override
    public Iterator<FedoraObjectProcessor> iterator() {
        return new FoxmlDirectoryDFSIterator(root, fetcher);
    }
}
