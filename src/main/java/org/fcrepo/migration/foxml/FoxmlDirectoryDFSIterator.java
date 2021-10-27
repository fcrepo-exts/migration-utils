/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.foxml;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import javax.xml.stream.XMLStreamException;

import org.fcrepo.migration.FedoraObjectProcessor;

/**
 * A depth-first-search iteration over a tree of files that exposes them as FedoraObjectProcessors.
 * Each file in the tree is expected to be a FOXML file.   This implementation likely minimizes
 * memory usage for the expected organization of FOXML files on disk.
 * @author mdurbin
 */
public class FoxmlDirectoryDFSIterator implements Iterator<FedoraObjectProcessor> {

    private List<File> current;
    private Stack<List<File>> stack;

    private InternalIDResolver resolver;
    private URLFetcher fetcher;

    private String localFedoraServer;

    private FileFilter fileFilter;

    /**
     * foxml directory DFS iterator.
     * @param root the root file
     * @param fetcher the fetcher
     * @param localFedoraServer uri to local fedora server
     * @param fileFilter a FileFilter that defined which files should be included
     *        in this Iterator.
     */
    public FoxmlDirectoryDFSIterator(final File root, final URLFetcher fetcher, final String localFedoraServer,
            final FileFilter fileFilter) {
        stack = new Stack<List<File>>();
        current = new ArrayList<File>(Arrays.asList(root.listFiles()));
        this.fetcher = fetcher;
        this.localFedoraServer = localFedoraServer;
        this.fileFilter = fileFilter;
    }

    /**
     * foxml directory DFS iterator with three parameters
     * @param root the root file
     * @param resolver the resolver
     * @param fetcher the fetcher
     * @param localFedoraServer the domain and port for the server that hosted the fedora objects in the format
     *                          "localhost:8080".
     * @param fileFilter a FileFilter that defined which files should be included
     *        in this Iterator.
     */
    public FoxmlDirectoryDFSIterator(final File root, final InternalIDResolver resolver, final URLFetcher fetcher,
                                     final String localFedoraServer, final FileFilter fileFilter) {
        this(root, fetcher, localFedoraServer, fileFilter);
        this.resolver = resolver;
    }

    private boolean advanceToNext() {
        while (current.size() > 0 || stack.size() > 0) {
            if (current.isEmpty()) {
                current = stack.pop();
            } else {
                final File first = current.get(0);
                if (first.isFile()) {
                    if (this.fileFilter.accept(first)) {
                        return true;
                    } else {
                        // exclude the current file and get the next one...
                        current.remove(0);
                        return advanceToNext();
                    }
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
            final File currentFile = current.remove(0);
            try {
                return new FoxmlInputStreamFedoraObjectProcessor(
                        currentFile, fetcher, resolver, localFedoraServer);
            } catch (final XMLStreamException e) {
                throw new RuntimeException(currentFile.getPath() + " doesn't appear to be an XML file."
                        + (e.getMessage() != null ? "  (" + e.getMessage() + ")" : ""));
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
