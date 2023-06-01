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
import org.fcrepo.storage.ocfl.OcflObjectSessionFactory;
import org.junit.After;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author pwinckles
 */
public class InlineXmlIT {

    private static final String OBJECT_ID = "info:fedora/1711.dl:CModelAudioStream";
    private static final String AUDIT_ID = OBJECT_ID + "/AUDIT";
    private static final String DC_ID = OBJECT_ID + "/DC";
    private static final String DS_COMPOSITE_MODEL_ID = OBJECT_ID + "/DS-COMPOSITE-MODEL";
    private static final String RELS_EXT_ID = OBJECT_ID + "/RELS-EXT";

    private static final Map<String, String> EXPECTED_DIGESTS = Map.of(
            AUDIT_ID, "c5aa5d74afc74aaf769b685e08d32cbc",
            DC_ID, "9b3cb6287c11be2eddd3ff0a66805103",
            DS_COMPOSITE_MODEL_ID, "84184d2d8ee6eae9dbcc5f02eaff681c",
            RELS_EXT_ID, "c30c3df0877b8f113f8f4f844bdfe3e6"
    );

    private ConfigurableApplicationContext context;
    private Migrator migrator;
    private OcflObjectSessionFactory sessionFactory;

    @After
    public void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    private void setup(final String name) throws Exception {
        // Create directories expected in this test (based on `spring/ocfl-user-it-setup.xml`)
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
        sessionFactory = (OcflObjectSessionFactory) context.getBean("ocflSessionFactory");
    }

    @Test
    public void testDcProperties() throws Exception {
        setup("inline-it");
        migrator.run();
        final var session = sessionFactory.newSession("info:fedora/fedora-system:ContentModel-3.0");
        final var content = session.readContent("info:fedora/fedora-system:ContentModel-3.0");
        final String contentText = new BufferedReader(
            new InputStreamReader(content.getContentStream().get(), StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));
        assertTrue(contentText.contains("<info:fedora/fedora-system:ContentModel-3.0> " +
                    "<http://purl.org/dc/elements/1.1/identifier> \"fedora-system:ContentModel-3.0\""));
    }

    @Test
    public void testMigrateObjectWithInlineXml() throws Exception {
        setup("inline-it");

        migrator.run();

        final var session = sessionFactory.newSession(OBJECT_ID);

        EXPECTED_DIGESTS.forEach((id, expected) -> {
            final var content = session.readContent(id);
            try {
                final var actual = DigestUtils.md5Hex(content.getContentStream().get());
                assertEquals(id, expected, actual);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Test
    public void failMigrationWhenInlineXmlDoesNotMatchDigest() throws Exception {
        setup("inline-invalid-it");

        try {
            migrator.run();
            fail("Expected migrator to fail");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage()
                    .contains("DC failed checksum validation. Expected MD5: 4e2a6140aa1369de6dd9736dfa8ab946"));
        }
    }


}
