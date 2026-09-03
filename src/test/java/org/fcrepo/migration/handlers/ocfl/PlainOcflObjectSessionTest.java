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

import io.ocfl.api.MutableOcflRepository;
import io.ocfl.api.model.ObjectVersionId;
import io.ocfl.core.OcflRepositoryBuilder;
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import io.ocfl.core.path.mapper.LogicalPathMappers;
import io.ocfl.core.storage.OcflStorageBuilder;
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
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
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
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .logicalPathMapper(logicalPathMapper)
                .storage(OcflStorageBuilder.builder().fileSystem(ocflRoot).build())
                .workDir(staging)
                .buildMutable();

        plainSessionFactory = new PlainOcflObjectSessionFactory(ocflRepo, staging,
                "testing", "fedoraAdmin", "info:fedora/fedoraAdmin", false);
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

    @Test
    public void identifiersAreExposed() {
        final var session = newSession();
        assertEquals(AG_ID, session.ocflObjectId());
        assertTrue(session.isOpen());
    }

    @Test
    public void invalidateCacheAndCloseAreSupported() {
        final var session = newSession();
        session.invalidateCache(AG_ID);
        session.close();
        assertFalse(session.isOpen());
    }

    @Test
    public void unsupportedOperationsThrow() {
        final var session = newSession();
        assertThrows(UnsupportedOperationException.class, () -> session.writeHeaders(null));
        assertThrows(UnsupportedOperationException.class, () -> session.deleteResource("x"));
        assertThrows(UnsupportedOperationException.class, () -> session.readHeaders("x"));
        assertThrows(UnsupportedOperationException.class, () -> session.readHeaders("x", "v1"));
        assertThrows(UnsupportedOperationException.class, () -> session.readContent("x"));
        assertThrows(UnsupportedOperationException.class, () -> session.readContent("x", "v1"));
        assertThrows(UnsupportedOperationException.class, () -> session.readRange("x", "v1", 0L, 1L));
        assertThrows(UnsupportedOperationException.class, () -> session.readRange("x", 0L, 1L));
        assertThrows(UnsupportedOperationException.class, () -> session.listVersions("x"));
        assertThrows(UnsupportedOperationException.class, () -> session.streamResourceHeaders());
        assertThrows(UnsupportedOperationException.class, () -> session.commitType(null));
        assertThrows(UnsupportedOperationException.class, session::rollback);
    }

    @Test
    public void containsResourceReflectsRepository() {
        final var probe = newSession();
        assertFalse(probe.containsResource(AG_ID));
        probe.abort();

        final var session = newSession();
        write(binary(AG_ID + "/bar", "test"), session);
        session.commit();

        assertTrue(newSession().containsResource(AG_ID));
    }

    @Test
    public void writeAfterCommitThrows() {
        final var session = newSession();
        write(binary(AG_ID + "/bar", "test"), session);
        session.commit();

        assertThrows(IllegalStateException.class,
                () -> write(binary(AG_ID + "/bar", "again"), session));
    }

    @Test
    public void aclInteractionModelIsUnsupported() {
        final var headers = headers(AG_ID + "/acl");
        headers.withInteractionModel(InteractionModel.ACL.getUri());
        final var session = newSession();
        assertThrows(UnsupportedOperationException.class,
                () -> session.writeResource(headers.build(), null));
    }

    @Test
    public void missingInteractionModelThrows() {
        final var headers = headers(AG_ID + "/none");
        final var session = newSession();
        assertThrows(IllegalArgumentException.class,
                () -> session.writeResource(headers.build(), null));
    }

    @Test
    public void versionMetadataIsPersisted() {
        final var session = newSession();
        session.versionAuthor("Migrator", "info:fedora/migrator");
        session.versionMessage("initial migration");
        write(binary(AG_ID + "/bar", "test"), session);
        session.commit();

        final var versionInfo = ocflRepo.describeVersion(ObjectVersionId.head(AG_ID)).getVersionInfo();
        assertEquals("initial migration", versionInfo.getMessage());
        assertEquals("Migrator", versionInfo.getUser().getName());
    }

    @Test
    public void writeWithValidChecksumRegistersFixity() throws Exception {
        final var dsId = "bar";
        final var resourceId = AG_ID + "/" + dsId;
        final var value = "checksummed";
        final var digest = sha512Hex(value);

        final var headers = headers(resourceId);
        headers.withInteractionModel(InteractionModel.NON_RDF.getUri());
        headers.withDigests(List.of(URI.create("urn:sha-512:" + digest)));
        final var content = new ResourceContent(IOUtils.toInputStream(value), headers.build());

        final var session = newSession();
        write(content, session);
        session.commit();

        assertEquals(value,
                IOUtils.toString(ocflRepo.getObject(ObjectVersionId.head(AG_ID)).getFile(dsId).getStream()));
    }

    @Test
    public void deleteContentFileRemovesFileOnCommit() {
        final var dsId = "bar";
        final var writeSession = newSession();
        write(binary(AG_ID + "/" + dsId, "test"), writeSession);
        writeSession.commit();
        assertTrue(ocflRepo.getObject(ObjectVersionId.head(AG_ID)).containsFile(dsId));

        final var deleteHeaders = headers(AG_ID + "/" + dsId);
        deleteHeaders.withInteractionModel(InteractionModel.NON_RDF.getUri());
        final var deleteSession = newSession();
        deleteSession.deleteContentFile(deleteHeaders.build());
        deleteSession.commit();

        assertFalse(ocflRepo.getObject(ObjectVersionId.head(AG_ID)).containsFile(dsId));
    }

    private static String sha512Hex(final String value) throws NoSuchAlgorithmException {
        final var digest = MessageDigest.getInstance("SHA-512");
        final var bytes = digest.digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        final var sb = new StringBuilder();
        for (final byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
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
