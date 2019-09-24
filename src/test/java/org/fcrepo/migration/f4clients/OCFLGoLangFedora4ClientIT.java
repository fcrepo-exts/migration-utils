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

import org.fcrepo.migration.Fedora4Client;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.ByteArrayInputStream;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *  A basic suite of integration tests to test certain interaction
 *  patterns (and code) against the an OCFL version of Fedora.
 *
 * @author awoods
 */
public class OCFLGoLangFedora4ClientIT {

    private Fedora4Client client;

    @Before
    public void setup() throws BeansException {
        final ConfigurableApplicationContext context =
                new ClassPathXmlApplicationContext("spring/ocfl-go-it-setup.xml");
        client = (Fedora4Client) context.getBean("fedora4Client");
    }

    @Test
    public void testExists() {
        final String id = UUID.randomUUID().toString();
        assertFalse("Object should not exist: " + id, client.exists(id));
        client.createResource(id);

        // Still should not exist... in the OCFL storage root
        assertFalse("Object should not exist: " + id, client.exists(id));

        client.createVersionSnapshot(id, "v1");
        assertTrue("Object should exist: " + id, client.exists(id));
    }

    @Test
    @Ignore
    public void testUnnamedRDFPlaceholder() {
        final String path = client.createPlaceholder(null);
        assertTrue(client.exists(path));
        assertTrue(client.isPlaceholder(path));
    }

    @Test
    @Ignore
    public void testNamedRDFPlaceholder() {
        final String path = UUID.randomUUID().toString();
        assertEquals(path, client.createPlaceholder(path));
        assertTrue(client.exists(path));
        assertTrue(client.isPlaceholder(path));
    }

    @Test
    @Ignore
    public void testNamedNonRDFPlaceholder() {
        final String path = UUID.randomUUID().toString();
        assertEquals(path, client.createNonRDFPlaceholder(path));
        assertTrue(client.exists(path));
        assertTrue(client.isPlaceholder(path));
        client.createOrUpdateNonRDFResource(path,
                new ByteArrayInputStream("<sample>xml</sample>".getBytes()), "text/xml");
    }
}
