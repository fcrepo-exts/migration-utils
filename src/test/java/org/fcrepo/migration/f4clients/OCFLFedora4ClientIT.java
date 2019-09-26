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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.fcrepo.migration.Fedora4Client;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.apache.commons.io.FilenameUtils;

/**
 * A basic suite of integration tests to test certain interaction patterns (and code) against the an OCFL version of
 * Fedora.
 *
 * @author awoods, copied from org.fcrepo.migration.f4clients.OCFLGoLangFedora4ClientIT.java by Remi Malessa
 * @since 4.4.1-SNAPSHOT
 */
public class OCFLFedora4ClientIT {

    private Fedora4Client client;

    @Before
    public void setup() throws BeansException {

        final ConfigurableApplicationContext context = new ClassPathXmlApplicationContext("spring/ocfl-it-setup.xml");
        client = (Fedora4Client) context.getBean("fedora4Client");
    }

    /**
     * Testing integration of the exist() method.
     *
     * @author awoods, copied from org.fcrepo.migration.f4clients.OCFLGoLangFedora4ClientIT.java by Remi Malessa
     * @since 4.4.1-SNAPSHOT
     */
    @Test
    public void testExists() {

        final String id = UUID.randomUUID().toString();
        assertFalse("Object should not exist: " + id, client.exists(id));

        // TODO implement integration test with client.createResource(id) method

        // TODO implement integration test with client.createVersionSnapshot(id, "v1")
    }

    /**
     * Testing integration of the createPlaceholder() method.
     * 
     * @author Dan Field
     * @since 4.4.1-SNAPSHOT
     */
    @Test
    public void testCreatePlaceholder() {
        final String newFileName = UUID.randomUUID().toString();
        final String returnedPath = client.createPlaceholder(newFileName);
        final String baseName = FilenameUtils.getBaseName(returnedPath);
        // check returned file really exists
        assertTrue("file " + baseName + " should exist as " + newFileName, client.exists(newFileName));
    }

    @Test
    public void testCreateResource() {

        final String id = UUID.randomUUID().toString();
        client.createResource(id);
    }
}
