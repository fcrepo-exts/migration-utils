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
package org.fcrepo.migration.handlers;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import junit.framework.Assert;
import org.fcrepo.client.FedoraDatastream;
import org.fcrepo.client.FedoraException;
import org.fcrepo.client.FedoraObject;
import org.fcrepo.client.impl.FedoraRepositoryImpl;
import org.fcrepo.migration.Fedora4Client;
import org.fcrepo.migration.MigrationIDMapper;
import org.fcrepo.migration.Migrator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.xml.stream.XMLStreamException;
import java.util.Iterator;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author mdurbin
 */
public class BasicObjectVersionHandlerIT {

    final static Logger LOGGER = getLogger(BasicObjectVersionHandlerIT.class);

    private static Fedora4Client client;

    private static FedoraRepositoryImpl repo;

    private static MigrationIDMapper idMapper;

    @BeforeClass
    public static void migrateTestData() throws XMLStreamException {
        final ConfigurableApplicationContext context =
                new ClassPathXmlApplicationContext("spring/it-setup.xml");

        ((Migrator) context.getBean("f3Migrator")).run();
        ((Migrator) context.getBean("f2Migrator")).run();

        client = (Fedora4Client) context.getBean("fedora4Client");
        repo = (FedoraRepositoryImpl) context.getBean("repo");
        idMapper = (MigrationIDMapper) context.getBean("idMapper");
        context.close();
    }

    @Test
    public void testObjectsWereCreated() throws FedoraException {
        Assert.assertTrue(client.exists(idMapper.mapObjectPath("example:1")));
        Assert.assertTrue(client.exists(idMapper.mapObjectPath("example:2")));
        Assert.assertTrue(client.exists(idMapper.mapObjectPath("example:3")));
    }

    @Test
    public void testPlaceholdersWereCreated() throws FedoraException {
        Assert.assertTrue(client.exists(idMapper.mapObjectPath("cmodle:1")));
    }

    @Test
    public void testExample1DatastreamsWereCreated() throws FedoraException {
        for (String dsid : new String[] { "DS1", "DS2", "DS3", "DS4" }) {
            Assert.assertTrue(client.exists(idMapper.mapDatastreamPath("example:1", dsid)));
        }
    }

    @Test
    public void testCustomPropertyMapping() throws FedoraException {
        final FedoraObject object = repo.getObject(idMapper.mapObjectPath("example:1"));
        final Iterator<Triple> tripleIt = object.getProperties();
        while (tripleIt.hasNext()) {
            final Triple next = tripleIt.next();
            if (next.predicateMatches(NodeFactory.createURI("http://fake/fake/pid"))
                    && next.objectMatches(NodeFactory.createLiteral("example:1"))) {
                return;
            } else {
                if (next.objectMatches(NodeFactory.createLiteral("example:1"))) {
                    LOGGER.debug(next.getPredicate().getURI() + " -> " + next.getObject().toString());
                }
            }
        }
        Assert.fail("Unable to find mapped PID.");
    }

    @Test
    public void testDatastreamProperties() throws FedoraException {
        final FedoraDatastream ds = repo.getDatastream(idMapper.mapDatastreamPath("example:1", "DS1"));
        final Iterator<Triple> tripleIt = ds.getProperties();
        while (tripleIt.hasNext()) {
            final Triple next = tripleIt.next();
            if (next.predicateMatches(NodeFactory.createURI("http://purl.org/dc/terms/title"))
                    && next.objectMatches(NodeFactory.createLiteral("Example inline XML datastream"))) {
                return;
            } else {
                Assert.assertFalse("Label was found under unexpected property! (\""
                        + next.getPredicate().getURI() + "\")",
                        next.objectMatches(NodeFactory.createLiteral("Example inline XML datastream")));
            }
        }
        Assert.fail("Unable to find datastream label in migrated resource RDF assertions.");
    }

}
