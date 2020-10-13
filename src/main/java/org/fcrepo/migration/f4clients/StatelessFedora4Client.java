/**
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoHttpClientBuilder;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.HttpMethods;
import org.fcrepo.migration.Fedora4Client;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * A Fedora4Client implementation that uses the code from the
 * fcrepo-camel project when possible.
 * This client is meant to be minimal in that it makes as few
 * requests as possible to implement the interface.
 * @author mdurbin
 */
public class StatelessFedora4Client implements Fedora4Client {

    private static final Logger LOGGER = getLogger(StatelessFedora4Client.class);

    private String baseUri;

    private String username;

    private String password;

    /**
     * Constructor for repositories for which Authentication is disabled;
     * requires the base URL for the Fedora 4 repository.
     * @param fcrepoBaseURL the base URL for a Fedora 4 repository.
     */
    public StatelessFedora4Client(final String fcrepoBaseURL) {
        baseUri = fcrepoBaseURL;
    }

    /**
     * Constructor for repositories for which Authentication is not disabled;
     * requires the base URL for the Fedora 4 repository.
     * @param username the username to authenticate with.
     * @param password the password to authenticate with.
     * @param fcrepoBaseURL the base URL for a Fedora 4 repository.
     */
    public StatelessFedora4Client(final String username, final String password, final String fcrepoBaseURL) {
        baseUri = fcrepoBaseURL;
        this.username = username;
        this.password = password;
    }

    private FcrepoClient getClient() {
        try {
            return new FcrepoClient.FcrepoClientBuilder()
                    .credentials(username, password)
                    .authScope(new URI(baseUri).toURL().getHost()).build();
        } catch (MalformedURLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean success(final FcrepoResponse r) {
        return r.getStatusCode() >= 200 && r.getStatusCode() < 300;
    }

    private void assertSuccess(final FcrepoResponse r) {
        if (!success(r)) {
            throw new RuntimeException("error code " + r.getStatusCode() + " from request " + r.getUrl());
        }
    }

    private URI pathToURI(final String path) throws URISyntaxException {
        return path.startsWith(baseUri) ? new URI(path) : new URI(baseUri + path);
    }

    private String uriToPath(final String URI) {
        if (URI.startsWith(baseUri)) {
            return URI.substring(baseUri.length());
        }
        return URI;
    }

    @Override
    public boolean exists(final String path) {
        try {
            return getClient().head(pathToURI(path)).perform().getStatusCode() != 404;
        } catch (FcrepoOperationFailedException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createResource(final String path) {
        try {
            assertSuccess(getClient().put(pathToURI(path)).perform());
        } catch (FcrepoOperationFailedException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getRepositoryUrl() {
        return baseUri.toString();
    }

    @Override
    public void createOrUpdateRedirectNonRDFResource(final String path, final String url) {
        try {

            assertSuccess(getClient().put(pathToURI(path))
                    .body((InputStream) null, "message/external-body; access-type=URL; URL=\"" + url.toString() + "\"")
                    .perform());
        } catch (FcrepoOperationFailedException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createOrUpdateNonRDFResource(final String path, final InputStream content, final String contentType) {
        try {
            assertSuccess(getClient().put(pathToURI(path)).body(content, contentType).perform());
        } catch (FcrepoOperationFailedException | URISyntaxException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                content.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public void createVersionSnapshot(final String path,final  String versionId) {
        try {
            final FcrepoHttpClientBuilder client
                    = new FcrepoHttpClientBuilder(username, password, new URI(baseUri).toURL().getHost());
            try (final CloseableHttpClient c = client.build()) {
                final HttpMethods method = HttpMethods.POST;
                final URI uri = pathToURI(path + "/fcr:versions");
                final HttpEntityEnclosingRequestBase request
                        = (HttpEntityEnclosingRequestBase) method.createRequest(uri);
                request.addHeader("Slug", versionId);
                try (final CloseableHttpResponse response = c.execute(request)) {
                    final int statusCode = response.getStatusLine().getStatusCode();
                    if (!(statusCode >= 200 && statusCode < 300)) {
                        throw new RuntimeException("Unable to create version! " + response.getStatusLine().toString());
                    }
                }
            }
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateResourceProperties(final String path, final String sparqlUpdate) {
        try {
            assertSuccess(getClient().patch(pathToURI(path))
                    .body(new ByteArrayInputStream(sparqlUpdate.getBytes("UTF-8"))).perform());
        } catch (FcrepoOperationFailedException | UnsupportedEncodingException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateNonRDFResourceProperties(final String path, final String sparqlUpdate) {
        try {
            assertSuccess(getClient().patch(pathToURI(path + "/fcr:metadata"))
                    .body(new ByteArrayInputStream(sparqlUpdate.getBytes("UTF-8"))).perform());
        } catch (FcrepoOperationFailedException | UnsupportedEncodingException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String createPlaceholder(final String path) {
        if (path == null || path.length() == 0) {
            try {
                final FcrepoResponse r = getClient().post(new URI(baseUri)).perform();
                assertSuccess(r);
                return uriToPath(r.getLocation().toString());
            } catch (FcrepoOperationFailedException | URISyntaxException e) {
                throw new RuntimeException(e);
            }
        } else if (!exists(path)) {
            createResource(path);
        }
        return path;
    }

    @Override
    public String createNonRDFPlaceholder(final String path) {
        if (!exists(path)) {
            if (path == null || path.length() == 0) {
                try {
                    final FcrepoResponse r = getClient().post(new URI(baseUri))
                            .body((InputStream) null, "text/plain").perform();
                    assertSuccess(r);
                    return uriToPath(r.getLocation().toString());
                } catch (FcrepoOperationFailedException | URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            } else {
                try {
                    assertSuccess(getClient().put(pathToURI(path))
                            .body(new ByteArrayInputStream("".getBytes("UTF-8")), "text/xml").perform());
                } catch (FcrepoOperationFailedException | URISyntaxException | UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
                return path;
            }
        } else {
            return path;
        }
    }

    @Override
    public boolean isPlaceholder(final String path) {
        try {
            return getClient().get(pathToURI(path + "/fcr:versions")).perform().getStatusCode() == 404;
        } catch (FcrepoOperationFailedException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

}
