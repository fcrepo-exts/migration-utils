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

package org.fcrepo.migration;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Dan Field
 */
public class DisabledDigestIT {

    private ConfigurableApplicationContext context;
    private Migrator migrator;

    @After
    public void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    private void setup(final String name) throws Exception {
        final var storage = Paths.get(String.format("target/test/ocfl/%s/storage", name));
        final var staging = Paths.get(String.format("target/test/ocfl/%s/staging", name));

        if (Files.exists(storage)) {
            FileUtils.forceDelete(storage.toFile());
        }
        if (Files.exists(staging)) {
            FileUtils.forceDelete(staging.toFile());
        }

        Files.createDirectories(storage);
        Files.createDirectories(staging);

        context = new ClassPathXmlApplicationContext(String.format("spring/%s-setup.xml", name));
        migrator = (Migrator) context.getBean("migrator");
    }

    @Test
    public void testMigrateObjectWithExternalDatastreamAndDisabledDigest() throws Exception {
        setup("inline-disabled-it");
        try {
            migrator.run();
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("DISABLED digest. Skipping digest validation"));
        }
    }

}
