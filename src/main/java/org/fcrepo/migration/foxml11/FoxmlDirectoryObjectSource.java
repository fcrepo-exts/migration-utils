package org.fcrepo.migration.foxml11;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import javax.xml.stream.XMLStreamException;

import org.fcrepo.migration.FedoraObjectProcessor;
import org.fcrepo.migration.ObjectSource;

/**
 * An ObjectSource implementation that exposes FOXML from a provided directory.
 */

public class FoxmlDirectoryObjectSource implements ObjectSource {

    private InternalIDResolver resolver;

    private URLFetcher fetcher;

    private File root;

    public FoxmlDirectoryObjectSource() {

    }

    /**
     * A constructor for use with the data storage directories that underly a
     * fedora 3.x repository.  First, this constructor will build an index of
     * all of the datastreams in the provided datastreamStore directory for use
     * resolving internal id references within the foxml.
     * @param objectStore a directory containing just directories and foxml files
     * @param datastreamStore a directory containing just directories and datastream files
     * @param workingDir a directory that may be used to cache working information, such as
     *                   an index of the datastream identifiers to file paths.
     */

    public FoxmlDirectoryObjectSource(final String objectStore, final String datastreamStore, final String workingDir) throws IOException {

        this.root = new File(objectStore);

        if ((datastreamStore == null ) || (workingDir == null)) {
            resolver = null;
        }
        else {

            resolver = new DirectoryScanningIDResolver(workingDir, datastreamStore);
        }
        fetcher = new HttpClientURLFetcher();

    }



    public void setResolver(final InternalIDResolver resolver) {
        this.resolver = resolver;
    }


    public void setFetcher(final URLFetcher fetcher) {
        this.fetcher = fetcher;
    }


    public void setRoot(final File root) {
        this.root = root;
    }

    @Override
    public Iterator<FedoraObjectProcessor> iterator() {
        return new FoxmlDirectoryDFSIterator(root);
    }


    /**
     * A depth-first-search iteration over a tree of files that exposes them as FedoraObjectProcessors.
     * Each file in the tree is expected to be a Foxml 1.1 file.   This implementation likely minimizes
     * memory usage for the expected organization of foxml files on disk.
     */
    private class FoxmlDirectoryDFSIterator implements Iterator<FedoraObjectProcessor> {

        private List<File> current;
        private Stack<List<File>> stack;

        public FoxmlDirectoryDFSIterator(final File root) {
            stack = new Stack<List<File>>();
            System.out.println("root path " + root.getAbsolutePath() );
            current = new ArrayList<File>(Arrays.asList(root.listFiles()));
        }

        private boolean advanceToNext() {
            while (current.size() > 0 || stack.size() > 0) {
                if (current.isEmpty()) {
                    current = stack.pop();
                } else {
                    final File first = current.get(0);
                    if (first.isFile()) {
                        return true;
                    } else {
                        final File directory = current.remove(0);
                        stack.push(current);
                        current = new ArrayList<File>(Arrays.asList(directory.listFiles()));
                    }
                }
            }
            return false;
        }

        @Override
        public boolean hasNext() {
            return advanceToNext();
        }

        @Override
        public FedoraObjectProcessor next() {
            if (!advanceToNext()) {
                throw new IllegalStateException();
            } else {
                try {
                    return new Foxml11InputStreamFedoraObjectProcessor(new FileInputStream(current.remove(0)), fetcher, resolver);
                } catch (final XMLStreamException e) {
                    throw new RuntimeException(e);
                } catch (final FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
