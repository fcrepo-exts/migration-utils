package org.fcrepo.migration.foxml11;

import java.io.File;
import java.util.Iterator;

import org.fcrepo.migration.FedoraObjectProcessor;
import org.fcrepo.migration.ObjectSource;

/**
 * An ObjectSource implementation that exposes FOXML from a provided directory.
 * The FOXML is expected to have been produced using the export API method with
 * the "archive" context.  As such, each file will be a completely self-contained
 * serialization of the Fedora 3 object.
 * @author mdurbin
 */
public class ArchiveExportedFoxmlDirectoryObjectSource implements ObjectSource {

    private File root;

    private URLFetcher fetcher;

    private String localFedoraServer;

    /**
     * archive exported foxml directory object source.
     * @param exportDir the export directory
     * @param localFedoraServer the domain and port for the server that hosted the fedora objects in the format
     *                          "localhost:8080".
     */
    public ArchiveExportedFoxmlDirectoryObjectSource(final File exportDir, final String localFedoraServer) {
        this.root = exportDir;
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
        return new FoxmlDirectoryDFSIterator(root, fetcher, localFedoraServer);
    }
}
