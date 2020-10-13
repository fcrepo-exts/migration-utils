/*
 * Copyright 2019 DuraSpace, Inc.
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

package org.fcrepo.migration.handlers.ocfl;

import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedTruncatedNTupleConfig;
import edu.wisc.library.ocfl.core.path.mapper.LogicalPathMappers;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.fcrepo.storage.ocfl.InteractionModel;
import org.fcrepo.storage.ocfl.OcflObjectSession;
import org.fcrepo.storage.ocfl.OcflObjectSessionFactory;
import org.fcrepo.storage.ocfl.ResourceContent;
import org.fcrepo.storage.ocfl.ResourceHeaders;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author pwinckles
 */
public class PlainOcflObjectSessionTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private Path ocflRoot;
    private Path staging;

    private MutableOcflRepository ocflRepo;
    private OcflObjectSessionFactory plainSessionFactory;

    private static final String AG_ID = "info:fedora/foo";

    @Before
    public void setup() throws IOException {
        ocflRoot = tempDir.newFolder("ocfl").toPath();
        staging = tempDir.newFolder("staging").toPath();

        final var logicalPathMapper = SystemUtils.IS_OS_WINDOWS ?
                LogicalPathMappers.percentEncodingWindowsMapper() : LogicalPathMappers.percentEncodingLinuxMapper();

        ocflRepo = new OcflRepositoryBuilder()
                .layoutConfig(new HashedTruncatedNTupleConfig())
                .logicalPathMapper(logicalPathMapper)
                .storage(FileSystemOcflStorage.builder().repositoryRoot(ocflRoot).build())
                .workDir(staging)
                .buildMutable();

        plainSessionFactory = new PlainOcflObjectSessionFactory(ocflRepo, staging,
                "testing", "fedoraAdmin", "info:fedora/fedoraAdmin");
    }

    @Test
    public void writeBinary() throws IOException {
        final var dsId = "bar";
        final var resourceId = AG_ID + "/" + dsId;
        final var content = binary(resourceId, "test");

        final var session = newSession();

        write(content, session);

        session.commit();

        assertEquals("test",
                IOUtils.toString(ocflRepo.getObject(ObjectVersionId.head(AG_ID)).getFile(dsId).getStream()));
    }

    @Test
    public void writeRdf() throws IOException {
        final var dsId = "baz";
        final var resourceId = AG_ID + "/" + dsId + "/fcr:metadata";
        final var content = rdf(resourceId, "desc");

        final var session = newSession();

        write(content, session);

        session.commit();

        assertEquals("desc",
                IOUtils.toString(ocflRepo.getObject(ObjectVersionId.head(AG_ID))
                        .getFile(dsId + "~fcr-desc.nt").getStream()));
    }

    @Test
    public void writeAg() throws IOException {
        final var content = rdf(AG_ID, "ag");

        final var session = newSession();

        write(content, session);

        session.commit();

        assertEquals("ag",
                IOUtils.toString(ocflRepo.getObject(ObjectVersionId.head(AG_ID))
                        .getFile("fcr-container.nt").getStream()));
    }

    @Test
    public void worksOnWindows() throws IOException {
        final var dsId = "bar:baz";
        final var resourceId = AG_ID + "/" + dsId;
        final var content = binary(resourceId, "test");

        final var session = newSession();

        write(content, session);

        session.commit();

        assertEquals("test",
                IOUtils.toString(ocflRepo.getObject(ObjectVersionId.head(AG_ID)).getFile(dsId).getStream()));
    }

    @Test
    public void cleanupOnAbort() {
        final var dsId = "bar:baz";
        final var resourceId = AG_ID + "/" + dsId;
        final var content = binary(resourceId, "test");

        final var session = newSession();

        write(content, session);

        session.abort();

        assertTrue(Files.notExists(staging.resolve(session.sessionId())));
    }

    private void write(final ResourceContent content, final OcflObjectSession session) {
        session.writeResource(content.getHeaders(), content.getContentStream().get());
    }

    private OcflObjectSession newSession() {
        return plainSessionFactory.newSession(AG_ID);
    }

    private ResourceContent ag(final String resourceId, final String content) {
        final var headers = headers(resourceId);
        headers.withInteractionModel(InteractionModel.BASIC_CONTAINER.getUri());
        headers.withArchivalGroup(true);
        return new ResourceContent(IOUtils.toInputStream(content), headers.build());
    }

    private ResourceContent binary(final String resourceId, final String content) {
        final var headers = headers(resourceId);
        headers.withInteractionModel(InteractionModel.NON_RDF.getUri());
        return new ResourceContent(IOUtils.toInputStream(content), headers.build());
    }

    private ResourceContent rdf(final String resourceId, final String content) {
        final var headers = headers(resourceId);
        headers.withInteractionModel(InteractionModel.NON_RDF_DESCRIPTION.getUri());
        return new ResourceContent(IOUtils.toInputStream(content), headers.build());
    }

    private ResourceHeaders.Builder headers(final String resourceId) {
        final var headers = ResourceHeaders.builder();
        headers.withId(resourceId);
        headers.withParent(AG_ID);
        return headers;
    }

}
