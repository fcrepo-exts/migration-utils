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
package org.fcrepo.migration.f4clients;

import com.hp.hpl.jena.graph.Triple;
import org.fcrepo.client.FedoraContent;
import org.fcrepo.client.FedoraException;
import org.fcrepo.client.FedoraRepository;
import org.fcrepo.kernel.api.RdfLexicon;
import org.fcrepo.migration.Fedora4Client;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;

/**
 * A Fedora4Client implementation built on top of the fcrepo4-client
 * project's FedoraRepositoryImpl class.
 *
 * @author Mike Durbin
 */
public class DefaultFedora4Client implements Fedora4Client {

    private FedoraRepository repo;

    /**
     * Only constructor; requires an instance of a FedoraRepository implementation.
     * @param repo a FedoraRepository implementation that will do the heavy lifting.
     */
    public DefaultFedora4Client(final FedoraRepository repo) {
        this.repo = repo;
    }

    @Override
    public boolean exists(final String path) {
        try {
            return repo.exists(path);
        } catch (FedoraException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createResource(final String path) {
        try {
            repo.createObject(path);
        } catch (FedoraException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getRepositoryUrl() {
        return repo.getRepositoryUrl();
    }

    @Override
    public void createOrUpdateRedirectNonRDFResource(final String path, final String url) {
        try {
            repo.createOrUpdateRedirectDatastream(path, url);
        } catch (FedoraException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createOrUpdateNonRDFResource(final String path, final InputStream content, final String contentType) {
        try {
            if (repo.exists(path)) {
                repo.getDatastream(path).updateContent(
                        new FedoraContent().setContent(content).setContentType(contentType));
            } else {
                repo.createDatastream(path, new FedoraContent().setContent(content).setContentType(contentType));
            }
        } catch (FedoraException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createVersionSnapshot(final String path, final String versionId) {
        try {
            repo.getObject(path).createVersionSnapshot(versionId);
        } catch (FedoraException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateResourceProperties(final String path, final String sparqlUpdate) {
        try {
            repo.getObject(path).updateProperties(sparqlUpdate);
        } catch (FedoraException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateNonRDFResourceProperties(final String path, final String sparqlUpdate) {
        try {
            repo.getDatastream(path).updateProperties(sparqlUpdate);
        } catch (FedoraException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String createPlaceholder(final String path) {
        try {
            if (!repo.exists(path)) {
                if (path == null || path.length() == 0) {
                    return repo.createResource(null).getPath();
                } else {
                    return repo.createObject(path).getPath();
                }
            } else {
                return path;
            }
        } catch (FedoraException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String createDSPlaceholder(final String path) {
        try {
            if (!repo.exists(path)) {
                if (path == null || path.length() == 0) {
                    return repo.createResource(null).getPath();
                } else {
                    try {
                        return repo.createDatastream(path,
                                new FedoraContent().setContent(
                                        new ByteArrayInputStream("placeholder".getBytes("UTF-8")))
                                        .setContentType("text/plain")).getPath();
                    } catch (UnsupportedEncodingException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } catch (FedoraException e) {
            throw new RuntimeException(e);
        }
        return path;
    }

    /**
     * {@inheritDoc}
     *
     * The current implementation uses the lazy approach of assuming any
     * resource that has versions is not a placeholder and otherwise is.
     *
     * TODO: make some migration-specific RDF assertion to more clearly mark
     *       placeholders.
     */
    @Override
    public boolean isPlaceholder(final String path) {
        try {
            final Iterator<Triple> properties = repo.getObject(path).getProperties();
            while (properties.hasNext()) {
                final Triple t = properties.next();
                if (t.predicateMatches(RdfLexicon.HAS_VERSION_HISTORY.asNode())) {
                    return false;
                }
            }
            return true;
        } catch (FedoraException e) {
            throw new RuntimeException(e);
        }
    }

}
