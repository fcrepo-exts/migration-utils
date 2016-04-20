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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.util.UUID;

import javax.xml.stream.XMLStreamException;

import org.fcrepo.migration.Fedora4Client;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 *  A basic suite of integration tests to test certain interaction
 *  patterns (and code) against the current version of Fedora 4.
 *
 *  All of the used functionality of Fedora4Client is tested in the
 *  BasicObjectVersionHandlerIT, but this class exists to help focus
 *  on the problem when such a test fails.
 * 
 * @author mdurbin
 */
public class StatelessFedora4ClientIT {

    private Fedora4Client client;

    @Before
    public void setup() throws BeansException, XMLStreamException {
        final ConfigurableApplicationContext context =
                new ClassPathXmlApplicationContext("spring/it-setup.xml");
        client = (Fedora4Client) context.getBean("fedora4Client");
    }

    @Test
    public void testExists() {
        final String id = UUID.randomUUID().toString();
        assertFalse(client.exists(id));
        client.createResource(id);
        assertTrue(client.exists(id));
    }

    @Test
    public void testUnnamedRDFPlaceholder() {
        final String path = client.createPlaceholder(null);
        assertTrue(client.exists(path));
        assertTrue(client.isPlaceholder(path));
    }

    @Test
    public void testNamedRDFPlaceholder() {
        final String path = UUID.randomUUID().toString();
        assertEquals(path, client.createPlaceholder(path));
        assertTrue(client.exists(path));
        assertTrue(client.isPlaceholder(path));
    }

    @Test
    public void testNamedNonRDFPlaceholder() {
        final String path = UUID.randomUUID().toString();
        assertEquals(path, client.createNonRDFPlaceholder(path));
        assertTrue(client.exists(path));
        assertTrue(client.isPlaceholder(path));
        client.createOrUpdateNonRDFResource(path,
                new ByteArrayInputStream("<sample>xml</sample>".getBytes()), "text/xml");
    }
}
