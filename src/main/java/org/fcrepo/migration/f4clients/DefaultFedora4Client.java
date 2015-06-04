package org.fcrepo.migration.f4clients;

import org.fcrepo.client.FedoraContent;
import org.fcrepo.client.FedoraException;
import org.fcrepo.client.FedoraRepository;
import org.fcrepo.migration.Fedora4Client;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

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
    public void createNonRDFResource(final String path) {
        try {
            repo.createDatastream(path, new FedoraContent().setContent(
                    new ByteArrayInputStream("placeholder".getBytes("UTF-8"))).setContentType("text/plain"));
        } catch (FedoraException e) {
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
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

}
