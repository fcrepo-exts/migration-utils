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
package org.fcrepo.migration.handlers;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.update.GraphStore;
import com.hp.hpl.jena.update.GraphStoreFactory;
import junit.framework.Assert;
import org.apache.http.client.ClientProtocolException;
import org.apache.jena.atlas.web.ContentType;
import org.apache.jena.riot.RDFLanguages;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.migration.Fedora4Client;
import org.fcrepo.migration.MigrationIDMapper;
import org.fcrepo.migration.Migrator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import static com.hp.hpl.jena.graph.Node.ANY;
import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author mdurbin
 */
public class BasicObjectVersionHandlerIT {

    final static Logger LOGGER = getLogger(BasicObjectVersionHandlerIT.class);

    private static Fedora4Client client;

    private static MigrationIDMapper idMapper;

    @BeforeClass
    public static void migrateTestData() throws XMLStreamException {
        final ConfigurableApplicationContext context =
                new ClassPathXmlApplicationContext("spring/it-setup.xml");

        ((Migrator) context.getBean("f3Migrator")).run();
        ((Migrator) context.getBean("f2Migrator")).run();

        client = (Fedora4Client) context.getBean("fedora4Client");
        idMapper = (MigrationIDMapper) context.getBean("idMapper");
        context.close();
    }

    @Test
    public void testObjectsWereCreated() {
        Assert.assertTrue(client.exists(idMapper.mapObjectPath("example:1")));
        Assert.assertTrue(client.exists(idMapper.mapObjectPath("example:2")));
        Assert.assertTrue(client.exists(idMapper.mapObjectPath("example:3")));
    }

    @Test
    public void testPlaceholdersWereCreated() {
        Assert.assertTrue(client.exists(idMapper.mapObjectPath("cmodle:1")));
    }

    @Test
    public void testExample1DatastreamsWereCreated() {
        for (String dsid : new String[] { "DS1", "DS2", "DS3", "DS4" }) {
            Assert.assertTrue(client.exists(idMapper.mapDatastreamPath("example:1", dsid)));
        }
    }

    @Test
    public void testCustomPropertyMapping() throws ClientProtocolException, URISyntaxException, IOException,
            FcrepoOperationFailedException {
        final GraphStore g = getResourceTriples(idMapper.mapObjectPath("example:1"));
        assertTrue("Unable to find mapped PID.",
                g.contains(ANY, ANY, NodeFactory.createURI("http://fake/fake/pid"),
                        NodeFactory.createLiteral("example:1")));
        g.close();
    }

    @Test
    public void testDatastreamProperties() throws ClientProtocolException, URISyntaxException, IOException,
            FcrepoOperationFailedException {
        final GraphStore g = getResourceTriples(idMapper.mapDatastreamPath("example:1", "DS1") + "/fcr:metadata");
        assertTrue("Unable to find datastream label in migrated resource RDF assertions.",
                g.contains(ANY, ANY, NodeFactory.createURI("http://purl.org/dc/terms/title"),
                        NodeFactory.createLiteral("Example inline XML datastream")));
    }

    private GraphStore getResourceTriples(final String path) throws URISyntaxException, IOException,
            FcrepoOperationFailedException {
        final FcrepoClient c = new FcrepoClient.FcrepoClientBuilder().build();
        final FcrepoResponse r = c.get(URI.create(client.getRepositoryUrl() + path)).perform();
        return parseTriples(r.getBody(), r.getContentType());
    }

    private static GraphStore parseTriples(final InputStream content, final String contentType) {
        final Model model = createDefaultModel();
        model.read(content, "", RDFLanguages.contentTypeToLang(ContentType.create(contentType)).getName());
        return GraphStoreFactory.create(model);
    }

}
