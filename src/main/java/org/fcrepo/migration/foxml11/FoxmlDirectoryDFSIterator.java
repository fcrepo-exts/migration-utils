package org.fcrepo.migration.foxml11;

import org.fcrepo.migration.FedoraObjectProcessor;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

/**
 * A depth-first-search iteration over a tree of files that exposes them as FedoraObjectProcessors.
 * Each file in the tree is expected to be a FOXML 1.1 file.   This implementation likely minimizes
 * memory usage for the expected organization of FOXML files on disk.
 */
public class FoxmlDirectoryDFSIterator implements Iterator<FedoraObjectProcessor> {

    private List<File> current;
    private Stack<List<File>> stack;

    private InternalIDResolver resolver;
    private URLFetcher fetcher;
    
    public FoxmlDirectoryDFSIterator(final File root, final URLFetcher fetcher) {
        stack = new Stack<List<File>>();
        current = new ArrayList<File>(Arrays.asList(root.listFiles()));
        this.fetcher = fetcher;
    }

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
