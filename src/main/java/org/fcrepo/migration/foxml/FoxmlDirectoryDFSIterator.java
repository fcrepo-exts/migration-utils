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

    /**
     * foxml directory DFS iterator.
     * @param root the root file
     * @param fetcher the fetcher
     * @param localFedoraServer uri to local fedora server
     */
    public FoxmlDirectoryDFSIterator(final File root, final URLFetcher fetcher, final String localFedoraServer) {
        stack = new Stack<List<File>>();
        current = new ArrayList<File>(Arrays.asList(root.listFiles()));
        this.fetcher = fetcher;
        this.localFedoraServer = localFedoraServer;
    }

    /**
     * foxml directory DFS iterator with three parameters
     * @param root the root file
     * @param resolver the resolver
     * @param fetcher the fetcher
     * @param localFedoraServer the domain and port for the server that hosted the fedora objects in the format
     *                          "localhost:8080".
     */
    public FoxmlDirectoryDFSIterator(final File root, final InternalIDResolver resolver, final URLFetcher fetcher,
                                     final String localFedoraServer) {
        this(root, fetcher, localFedoraServer);
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
                return new FoxmlInputStreamFedoraObjectProcessor(
                        new FileInputStream(current.remove(0)), fetcher, resolver, localFedoraServer);
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
