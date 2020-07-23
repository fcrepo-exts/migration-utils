package org.fcrepo.migration.handlers.ocfl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.fcrepo.migration.ContentDigest;
import org.fcrepo.migration.DatastreamInfo;
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.DefaultObjectInfo;
import org.fcrepo.migration.MigrationType;
import org.fcrepo.migration.ObjectProperties;
import org.fcrepo.migration.ObjectProperty;
import org.fcrepo.migration.ObjectVersionReference;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static org.fcrepo.migration.handlers.ocfl.ArchiveGroupHandler.BASIC_CONTAINER;
import static org.fcrepo.migration.handlers.ocfl.ArchiveGroupHandler.NON_RDF_SOURCE;
import static org.fcrepo.migration.handlers.ocfl.ArchiveGroupHandler.NON_RDF_SOURCE_DESCRIPTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author pwinckles
 */
@RunWith(MockitoJUnitRunner.Strict.class)
public class ArchiveGroupHandlerTest {

    private static final String FCREPO_ROOT = "info:fedora/";
    private static final String USER = "fedoraAdmin";

    @Mock
    private OcflDriver ocflDriver;

    private ObjectMapper objectMapper;

    private String date;

    @Before
    public void setup() {
        date = Instant.now().toString().substring(0, 10);
        objectMapper = new ObjectMapper()
                .configure(WRITE_DATES_AS_TIMESTAMPS, false)
                .registerModule(new JavaTimeModule())
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    @Test
    public void processObjectSingleVersionF6Format() {
        final var handler = createHandler(MigrationType.F6_OCFL, false);

        final var pid = "obj1";
        final var dsId1 = "ds1";
        final var dsId2 = "ds2";

        final var ds1 = datastreamVersion(dsId1, true, "M", "text/plain", "hello", null);
        final var ds2 = datastreamVersion(dsId2, true, "M", "text/plain", "goodbye", null);

        final var session = openSession(pid);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1, ds2))
        ));

        putObjectRdfAndVerify(session, pid);
        putObjectHeadersAndVerify(session, pid);

        putBinaryAndVerify(session, dsId1, ds1, 0);
        putHeadersAndVerify(session, dsId1, ds1, pid, 0);
        putF6RdfAndVerify(session, dsId1, ds1, 0);
        putDescHeadersAndVerify(session, dsId1, pid, 0);

        putBinaryAndVerify(session, dsId2, ds2, 0);
        putHeadersAndVerify(session, dsId2, ds2, pid, 0);
        putF6RdfAndVerify(session, dsId2, ds2, 0);
        putDescHeadersAndVerify(session, dsId2, pid, 0);

        verify(session).commit();
    }

    @Test
    public void processObjectMultipleVersionsF6Format() {
        final var handler = createHandler(MigrationType.F6_OCFL, false);

        final var pid = "obj2";
        final var dsId1 = "ds3";
        final var dsId2 = "ds4";

        final var ds1V1 = datastreamVersion(dsId1, true, "M", "application/xml", "<h1>hello</h1>", null);
        final var ds2V1 = datastreamVersion(dsId2, true, "M", "text/plain", "goodbye", null);

        final var ds2V2 = datastreamVersion(dsId2, false, "M", "text/plain", "fedora", null);

        final var session = openSession(pid);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1V1, ds2V1)),
                objectVersionReference(pid, false, List.of(ds2V2))
        ));

        putObjectRdfAndVerify(session, pid);
        putObjectHeadersAndVerify(session, pid);

        putBinaryAndVerify(session, dsId1, ds1V1, 0);
        putHeadersAndVerify(session, dsId1, ds1V1, pid, 0);
        putF6RdfAndVerify(session, dsId1, ds1V1, 0);
        putDescHeadersAndVerify(session, dsId1, pid, 0);

        putBinaryAndVerify(session, dsId2, ds2V1, 0);
        putHeadersAndVerify(session, dsId2, ds2V1, pid, 0);
        putF6RdfAndVerify(session, dsId2, ds2V1, 0);
        putDescHeadersAndVerify(session, dsId2, pid, 0);

        putBinaryAndVerify(session, dsId2, ds2V2, 1);
        putHeadersAndVerify(session, dsId2, ds2V2, pid, 1);
        putF6RdfAndVerify(session, dsId2, ds2V2, 1);
        putDescHeadersAndVerify(session, dsId2, pid, 1);

        verify(session, times(2)).commit();
    }

    @Test
    public void processObjectMultipleVersionsVanillaFormat() {
        final var handler = createHandler(MigrationType.VANILLA_OCFL, false);

        final var pid = "obj2";
        final var dsId1 = "ds3";
        final var dsId2 = "ds4";

        final var ds1V1 = datastreamVersion(dsId1, true, "M", "application/xml", "<h1>hello</h1>", null);
        final var ds2V1 = datastreamVersion(dsId2, true, "M", "text/plain", "goodbye", null);

        final var ds2V2 = datastreamVersion(dsId2, false, "M", "text/plain", "fedora", null);

        final var session = openSession(pid);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1V1, ds2V1)),
                objectVersionReference(pid, false, List.of(ds2V2))
        ));

        verify(session, never()).put(startsWith(".fcrepo"), any());

        putObjectRdfAndVerify(session, pid);

        putBinaryAndVerify(session, dsId1, ds1V1, 0);
        putVanillaRdfAndVerify(session, dsId1, ds1V1, 0);

        putBinaryAndVerify(session, dsId2, ds2V1, 0);
        putVanillaRdfAndVerify(session, dsId2, ds2V1, 0);

        putBinaryAndVerify(session, dsId2, ds2V2, 1);
        putVanillaRdfAndVerify(session, dsId2, ds2V2, 1);

        verify(session, times(2)).commit();
    }

    @Test
    public void processObjectSingleVersionF6FormatWithExtensions() {
        final var handler = createHandler(MigrationType.F6_OCFL, true);

        final var pid = "obj1";
        final var dsId1 = "ds1";
        final var dsId1Ext = "ds1.txt";
        final var dsId2 = "ds2";
        final var dsId2Ext = "ds2.rdf";
        final var dsId3 = "ds3";
        final var dsId3Ext = "ds3.jpg";

        final var ds1 = datastreamVersion(dsId1, true, "M", "text/plain", "text", null);
        final var ds2 = datastreamVersion(dsId2, true, "M", "application/rdf+xml", "xml", null);
        final var ds3 = datastreamVersion(dsId3, true, "M", "image/jpeg", "image", null);

        final var session = openSession(pid);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1, ds2, ds3))
        ));

        putObjectRdfAndVerify(session, pid);
        putObjectHeadersAndVerify(session, pid);

        putBinaryAndVerify(session, dsId1Ext, ds1, 0);
        putHeadersAndVerify(session, dsId1Ext, ds1, pid, 0);
        putF6RdfAndVerify(session, dsId1Ext, ds1, 0);
        putDescHeadersAndVerify(session, dsId1Ext, pid, 0);

        putBinaryAndVerify(session, dsId2Ext, ds2, 0);
        putHeadersAndVerify(session, dsId2Ext, ds2, pid, 0);
        putF6RdfAndVerify(session, dsId2Ext, ds2, 0);
        putDescHeadersAndVerify(session, dsId2Ext, pid, 0);

        putBinaryAndVerify(session, dsId3Ext, ds3, 0);
        putHeadersAndVerify(session, dsId3Ext, ds3, pid, 0);
        putF6RdfAndVerify(session, dsId3Ext, ds3, 0);
        putDescHeadersAndVerify(session, dsId3Ext, pid, 0);

        verify(session).commit();
    }

    @Test
    public void processObjectSingleVersionVanillaFormatWithExtensions() {
        final var handler = createHandler(MigrationType.VANILLA_OCFL, true);

        final var pid = "obj1";
        final var dsId1 = "ds1";
        final var dsId1Ext = "ds1.txt";
        final var dsId2 = "ds2";
        final var dsId2Ext = "ds2.rdf";
        final var dsId3 = "ds3";
        final var dsId3Ext = "ds3.jpg";

        final var ds1 = datastreamVersion(dsId1, true, "M", "text/plain", "text", null);
        final var ds2 = datastreamVersion(dsId2, true, "M", "application/rdf+xml", "xml", null);
        final var ds3 = datastreamVersion(dsId3, true, "M", "image/jpeg", "image", null);

        final var session = openSession(pid);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1, ds2, ds3))
        ));

        verify(session, never()).put(startsWith(".fcrepo"), any());

        putObjectRdfAndVerify(session, pid);

        putBinaryAndVerify(session, dsId1Ext, ds1, 0);
        putVanillaRdfAndVerify(session, dsId1Ext, ds1, 0);

        putBinaryAndVerify(session, dsId2Ext, ds2, 0);
        putVanillaRdfAndVerify(session, dsId2Ext, ds2, 0);

        putBinaryAndVerify(session, dsId3Ext, ds3, 0);
        putVanillaRdfAndVerify(session, dsId3Ext, ds3, 0);

        verify(session).commit();
    }

    @Test
    public void processObjectSingleVersionF6FormatWithExternalBinary() {
        final var handler = createHandler(MigrationType.F6_OCFL, false);

        final var pid = "obj1";
        final var dsId1 = "ds1";
        final var dsId2 = "ds2";
        final var dsId3 = "ds3";

        final var ds1 = datastreamVersion(dsId1, true, "M", "text/plain", "hello", null);
        final var ds2 = datastreamVersion(dsId2, true, "E", "text/plain", "", "https://external");
        final var ds3 = datastreamVersion(dsId3, true, "R", "text/plain", "", "https://redirect");

        final var session = openSession(pid);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1, ds2, ds3))
        ));

        putObjectRdfAndVerify(session, pid);
        putObjectHeadersAndVerify(session, pid);

        putBinaryAndVerify(session, dsId1, ds1, 0);
        putHeadersAndVerify(session, dsId1, ds1, pid, 0);
        putF6RdfAndVerify(session, dsId1, ds1, 0);
        putDescHeadersAndVerify(session, dsId1, pid, 0);

        verify(session, never()).put(startsWith(dsId2), any());
        putHeadersAndVerify(session, dsId2, ds2, pid, 0);

        verify(session, never()).put(startsWith(dsId3), any());
        putHeadersAndVerify(session, dsId3, ds3, pid, 0);

        verify(session).commit();
    }

    @Test
    public void processObjectSingleVersionVanillaFormatWithExternalBinary() {
        final var handler = createHandler(MigrationType.VANILLA_OCFL, false);

        final var pid = "obj1";
        final var dsId1 = "ds1";
        final var dsId2 = "ds2";
        final var dsId3 = "ds3";

        final var ds1 = datastreamVersion(dsId1, true, "M", "text/plain", "hello", null);
        final var ds2 = datastreamVersion(dsId2, true, "E", "text/plain", "", "https://external");
        final var ds3 = datastreamVersion(dsId3, true, "R", "text/plain", "", "https://redirect");

        final var session = openSession(pid);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1, ds2, ds3))
        ));

        verify(session, never()).put(startsWith(".fcrepo"), any());

        putObjectRdfAndVerify(session, pid);

        putBinaryAndVerify(session, dsId1, ds1, 0);
        putF6RdfAndVerify(session, dsId1, ds1, 0);

        putBinaryAndVerify(session, dsId2, ds2, 0);

        putBinaryAndVerify(session, dsId3, ds3, 0);

        verify(session).commit();
    }

    private void putBinaryAndVerify(final OcflSession session,
                                    final String path,
                                    final DatastreamVersion datastreamVersion,
                                    final int index) {
        final var captor = ArgumentCaptor.forClass(InputStream.class);
        verify(session, atLeastOnce()).put(eq(path), captor.capture());
        try {
            if ("RE".contains(datastreamVersion.getDatastreamInfo().getControlGroup())) {
                assertEquals(datastreamVersion.getExternalOrRedirectURL(),
                        IOUtils.toString(captor.getAllValues().get(index)));
            } else {
                assertSame(datastreamVersion.getContent(), captor.getAllValues().get(index));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void putHeadersAndVerify(final OcflSession session,
                                     final String path,
                                     final DatastreamVersion datastreamVersion,
                                     final String parentId,
                                     final int index) {
        final var captor = ArgumentCaptor.forClass(InputStream.class);
        verify(session, atLeastOnce()).put(eq(PersistencePaths.binaryHeaderPath(path)),
                captor.capture());
        try {
            final var headers = objectMapper.readValue(captor.getAllValues().get(index), ResourceHeaders.class);
            assertEquals(FCREPO_ROOT + parentId + "/" + path, headers.getId());
            assertEquals(FCREPO_ROOT + parentId, headers.getParent());
            assertEquals(NON_RDF_SOURCE, headers.getInteractionModel());
            assertFalse("not AG", headers.isArchivalGroup());
            assertFalse("not root", headers.isObjectRoot());
            assertFalse("not deleted", headers.isDeleted());
            assertEquals(USER, headers.getCreatedBy());
            assertEquals(USER, headers.getLastModifiedBy());
            assertThat(headers.getLastModifiedDate().toString(), containsString(date));
            assertThat(headers.getCreatedDate().toString(), containsString(date));
            assertEquals(Long.valueOf(datastreamVersion.getSize()), headers.getContentSize());
            assertEquals(datastreamVersion.getMimeType(), headers.getMimeType());
            assertEquals(path, headers.getFilename());
            assertEquals(DigestUtils.md5Hex(
                    String.valueOf(Instant.parse(datastreamVersion.getCreated()).toEpochMilli())).toUpperCase(),
                    headers.getStateToken());
            assertEquals(1, headers.getDigests().size());
            assertEquals(PersistencePaths.binaryContentPath(path), headers.getContentPath());
            assertEquals(datastreamVersion.getExternalOrRedirectURL(), headers.getExternalUrl());
            if (Objects.equals("R", datastreamVersion.getDatastreamInfo().getControlGroup())) {
                assertEquals("redirect", headers.getExternalHandling());
            } else if (Objects.equals("E", datastreamVersion.getDatastreamInfo().getControlGroup())) {
                assertEquals("proxy", headers.getExternalHandling());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void putDescHeadersAndVerify(final OcflSession session,
                                         final String dsId,
                                         final String objectId,
                                         final int index) {
        final var captor = ArgumentCaptor.forClass(InputStream.class);
        verify(session, atLeastOnce()).put(eq(PersistencePaths.binaryDescHeaderPath(dsId)),
                captor.capture());
        try {
            final var parentId = FCREPO_ROOT + objectId + "/" + dsId;
            final var headers = objectMapper.readValue(captor.getAllValues().get(index), ResourceHeaders.class);
            assertEquals(parentId + "/fcr:metadata", headers.getId());
            assertEquals(parentId, headers.getParent());
            assertEquals(NON_RDF_SOURCE_DESCRIPTION, headers.getInteractionModel());
            assertFalse("not AG", headers.isArchivalGroup());
            assertFalse("not root", headers.isObjectRoot());
            assertFalse("not deleted", headers.isDeleted());
            assertEquals(USER, headers.getCreatedBy());
            assertEquals(USER, headers.getLastModifiedBy());
            assertEquals(PersistencePaths.binaryDescContentPath(dsId), headers.getContentPath());
            assertThat(headers.getLastModifiedDate().toString(), containsString(date));
            assertThat(headers.getCreatedDate().toString(), containsString(date));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void putObjectHeadersAndVerify(final OcflSession session,
                                             final String objectId) {
        final var captor = ArgumentCaptor.forClass(InputStream.class);
        verify(session).put(eq(PersistencePaths.rootHeaderPath(objectId)), captor.capture());
        try {
            final var headers = objectMapper.readValue(captor.getValue(), ResourceHeaders.class);
            assertEquals(FCREPO_ROOT + objectId, headers.getId());
            assertEquals(FCREPO_ROOT, headers.getParent());
            assertEquals(BASIC_CONTAINER, headers.getInteractionModel());
            assertTrue("is AG", headers.isArchivalGroup());
            assertTrue("is root", headers.isObjectRoot());
            assertFalse("not deleted", headers.isDeleted());
            assertEquals(USER, headers.getCreatedBy());
            assertEquals(USER, headers.getLastModifiedBy());
            assertEquals(PersistencePaths.rootContentPath(objectId), headers.getContentPath());
            assertThat(headers.getLastModifiedDate().toString(), containsString(date));
            assertThat(headers.getCreatedDate().toString(), containsString(date));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void putF6RdfAndVerify(final OcflSession session,
                                   final String path,
                                   final DatastreamVersion datastreamVersion,
                                   final int index) {
        final var captor = ArgumentCaptor.forClass(InputStream.class);
        verify(session, atLeastOnce()).put(eq(PersistencePaths.binaryDescContentPath(path)),
                captor.capture());
        try {
            final var value = IOUtils.toString(captor.getAllValues().get(index));
            assertThat(value, allOf(
                    containsString(datastreamVersion.getLabel()),
                    containsString(datastreamVersion.getFormatUri()),
                    containsString("objState")
            ));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void putVanillaRdfAndVerify(final OcflSession session,
                                   final String path,
                                   final DatastreamVersion datastreamVersion,
                                   final int index) {
        final var captor = ArgumentCaptor.forClass(InputStream.class);
        verify(session, atLeastOnce()).put(eq(PersistencePaths.binaryDescContentPath(path)),
                captor.capture());
        try {
            final var value = IOUtils.toString(captor.getAllValues().get(index));
            assertThat(value, allOf(
                    containsString(datastreamVersion.getLabel()),
                    containsString(datastreamVersion.getFormatUri()),
                    containsString(datastreamVersion.getMimeType()),
                    containsString(path),
                    containsString("objState"),
                    containsString("hasMessageDigest"),
                    containsString("lastModified"),
                    containsString(date),
                    containsString("created")
            ));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void putObjectRdfAndVerify(final OcflSession session, final String path) {
        final var captor = ArgumentCaptor.forClass(InputStream.class);
        verify(session).put(eq(PersistencePaths.rootContentPath(path)), captor.capture());
        try {
            final var value = IOUtils.toString(captor.getValue());
            assertThat(value, allOf(
                    containsString("lastModifiedDate"),
                    containsString(date),
                    containsString("createdDate")
            ));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private ArchiveGroupHandler createHandler(final MigrationType migrationType, final boolean addExtensions) {
        return new ArchiveGroupHandler(ocflDriver, migrationType, addExtensions, USER);
    }

    private OcflSession openSession(final String pid) {
        final var mock = Mockito.mock(OcflSession.class);
        when(ocflDriver.open(FCREPO_ROOT + pid)).thenReturn(mock);
        return mock;
    }

    private ObjectVersionReference objectVersionReference(final String pid,
                                                          final boolean isFirst,
                                                          final List<DatastreamVersion> datastreamVersions) {
        final var mock = Mockito.mock(ObjectVersionReference.class);
        when(mock.getObjectInfo()).thenReturn(new DefaultObjectInfo(pid, pid));
        when(mock.isFirstVersion()).thenReturn(isFirst);
        if (isFirst) {
            final var properties = objectProperties(List.of(
                    objectProperty("info:fedora/fedora-system:def/view#lastModifiedDate", Instant.now().toString()),
                    objectProperty("info:fedora/fedora-system:def/model#createdDate", Instant.now().toString())
            ));
            when(mock.getObjectProperties()).thenReturn(properties);
        }
        when(mock.getObject()).thenReturn(null);
        when(mock.listChangedDatastreams()).thenReturn(datastreamVersions);
        return mock;
    }

    private ObjectProperties objectProperties(final List properties) {
        final var mock = Mockito.mock(ObjectProperties.class);
        when(mock.listProperties()).thenReturn(properties);
        return mock;
    }

    private ObjectProperty objectProperty(final String name, final String value) {
        final var mock = Mockito.mock(ObjectProperty.class);
        when(mock.getName()).thenReturn(name);
        when(mock.getValue()).thenReturn(value);
        return mock;
    }

    private DatastreamVersion datastreamVersion(final String datastreamId,
                                                final boolean isFirst,
                                                final String controlGroup,
                                                final String mimeType,
                                                final String content,
                                                final String externalUrl) {
        final var mock = Mockito.mock(DatastreamVersion.class);
        final var info = datastreamInfo(datastreamId, controlGroup);
        when(mock.getDatastreamInfo()).thenReturn(info);
        when(mock.getMimeType()).thenReturn(mimeType);
        try {
            when(mock.getContent()).thenReturn(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        when(mock.isFirstVersionIn(Mockito.isNull())).thenReturn(isFirst);
        when(mock.getCreated()).thenReturn(Instant.now().toString());
        when(mock.getExternalOrRedirectURL()).thenReturn(externalUrl);
        when(mock.getSize()).thenReturn((long) content.length());
        final var contentDigest = contentDigest(content);
        when(mock.getContentDigest()).thenReturn(contentDigest);
        when(mock.getLabel()).thenReturn(datastreamId + "-label");
        when(mock.getFormatUri()).thenReturn("http://format-id");
        return mock;
    }

    private DatastreamInfo datastreamInfo(final String datastreamId, final String controlGroup) {
        final var mock = Mockito.mock(DatastreamInfo.class);
        when(mock.getDatastreamId()).thenReturn(datastreamId);
        when(mock.getControlGroup()).thenReturn(controlGroup);
        when(mock.getState()).thenReturn("A");
        return mock;
    }

    private ContentDigest contentDigest(final String content) {
        final var mock = Mockito.mock(ContentDigest.class);
        when(mock.getType()).thenReturn("md5");
        when(mock.getDigest()).thenReturn(DigestUtils.md5Hex(content));
        return mock;
    }

}
