package org.fcrepo.migration.foxml11;

import java.io.File;
import java.io.FileInputStream;
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
 * Each file in the tree is expected to be a FOXML 1.1 file.   This implementation likely minimizes
 * memory usage for the expected organization of FOXML files on disk.
 * @author mdurbin
 */
public class FoxmlDirectoryDFSIterator implements Iterator<FedoraObjectProcessor> {

    private List<File> current;
    private Stack<List<File>> stack;

    private InternalIDResolver resolver;
    private URLFetcher fetcher;

    /**
     * foxml directory DFS iterator.
     * @param root the root file
     * @param fetcher the fetcher
     */
    public FoxmlDirectoryDFSIterator(final File root, final URLFetcher fetcher) {
        stack = new Stack<List<File>>();
        current = new ArrayList<File>(Arrays.asList(root.listFiles()));
        this.fetcher = fetcher;
    }

    /**
     * foxml directory DFS iterator with three parameters
     * @param root the root file
     * @param resolver the resolver
     * @param fetcher the fetcher
     */
    public FoxmlDirectoryDFSIterator(final File root, final InternalIDResolver resolver, final URLFetcher fetcher) {
        this(root, fetcher);
        this.resolver = resolver;
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
                return new Foxml11InputStreamFedoraObjectProcessor(
                        new FileInputStream(current.remove(0)), fetcher, resolver);
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
