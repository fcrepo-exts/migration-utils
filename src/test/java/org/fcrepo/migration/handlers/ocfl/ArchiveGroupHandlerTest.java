package org.fcrepo.migration.handlers.ocfl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.model.FileDetails;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import edu.wisc.library.ocfl.core.path.mapper.LogicalPathMappers;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.fcrepo.migration.ContentDigest;
import org.fcrepo.migration.DatastreamInfo;
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.DefaultObjectInfo;
import org.fcrepo.migration.MigrationType;
import org.fcrepo.migration.ObjectProperties;
import org.fcrepo.migration.ObjectProperty;
import org.fcrepo.migration.ObjectVersionReference;
import org.fcrepo.storage.ocfl.CommitType;
import org.fcrepo.storage.ocfl.DefaultOcflObjectSessionFactory;
import org.fcrepo.storage.ocfl.InteractionModel;
import org.fcrepo.storage.ocfl.OcflObjectSession;
import org.fcrepo.storage.ocfl.OcflObjectSessionFactory;
import org.fcrepo.storage.ocfl.PersistencePaths;
import org.fcrepo.storage.ocfl.ResourceHeadersVersion;
import org.fcrepo.storage.ocfl.cache.NoOpCache;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * @author pwinckles
 */
public class ArchiveGroupHandlerTest {

    private static final String FCREPO_ROOT = "info:fedora/";
    private static final String USER = "fedoraAdmin";
    private static final String INLINE = "X";
    private static final String PROXY = "E";
    private static final String REDIRECT = "R";
    private static final String MANAGED = "M";
    private static final String DS_ACTIVE = "A";
    private static final String DS_INACTIVE = "I";
    private static final String DS_DELETED = "D";
    private static final String OBJ_ACTIVE = "Active";
    private static final String OBJ_INACTIVE = "Inactive";
    private static final String OBJ_DELETED = "Deleted";
    private static final String RELS_INT = "RELS-INT";
    private static final String RELS_EXT = "RELS-EXT";

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private Path ocflRoot;
    private Path staging;

    private MutableOcflRepository ocflRepo;
    private OcflObjectSessionFactory sessionFactory;
    private OcflObjectSessionFactory plainSessionFactory;

    private ObjectMapper objectMapper;
    private String date;

    @Before
    public void setup() throws IOException {
        ocflRoot = tempDir.newFolder("ocfl").toPath();
        staging = tempDir.newFolder("staging").toPath();

        final var logicalPathMapper = SystemUtils.IS_OS_WINDOWS ?
                LogicalPathMappers.percentEncodingWindowsMapper() : LogicalPathMappers.percentEncodingLinuxMapper();

        ocflRepo = new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .logicalPathMapper(logicalPathMapper)
                .storage(FileSystemOcflStorage.builder().repositoryRoot(ocflRoot).build())
                .workDir(staging)
                .buildMutable();

        objectMapper = new ObjectMapper()
                .configure(WRITE_DATES_AS_TIMESTAMPS, false)
                .registerModule(new JavaTimeModule())
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);

        sessionFactory = new DefaultOcflObjectSessionFactory(ocflRepo, staging, objectMapper,
                new NoOpCache<>(), new NoOpCache<>(), CommitType.NEW_VERSION,
                "testing", USER, "info:fedora/fedoraAdmin");

        plainSessionFactory = new PlainOcflObjectSessionFactory(ocflRepo, staging,
                "testing", USER, "info:fedora/fedoraAdmin", false);

        date = Instant.now().toString().substring(0, 10);
    }

    @Test
    public void processObjectSingleVersionF6Format() throws IOException {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, false, false);

        final var pid = "obj1";
        final var dsId1 = "ds1";
        final var dsId2 = "ds2";

        final var ds1 = datastreamVersion(dsId1, true, MANAGED, "text/plain", "hello", null);
        final var ds2 = datastreamVersion(dsId2, true, INLINE, "application/xml", "<xml>goodbye</xml>", null);
        when(ds2.getSize()).thenReturn(100L);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1, ds2))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var ocflObjectId = addPrefix(pid);
        final var session = sessionFactory.newSession(ocflObjectId);

        verifyObjectRdf(contentToString(session, ocflObjectId));
        verifyObjectHeaders(session, ocflObjectId);

        verifyBinary(contentToString(session, ocflObjectId, dsId1), ds1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1);
        verifyDescRdf(session, ocflObjectId, dsId1, ds1);
        verifyDescHeaders(session, ocflObjectId, dsId1);

        verifyBinary(contentToString(session, ocflObjectId, dsId2), ds2);
        verifyHeaders(session, ocflObjectId, dsId2, ds2);
        verifyDescRdf(session, ocflObjectId, dsId2, ds2);
        verifyDescHeaders(session, ocflObjectId, dsId2);
    }

    @Test
    public void processObjectMultipleVersionsF6Format() throws IOException {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, false, false);

        final var pid = "obj2";
        final var dsId1 = "ds3";
        final var dsId2 = "ds4";

        final var ds1V1 = datastreamVersion(dsId1, true, MANAGED, "application/xml", "<h1>hello</h1>", null);
        final var ds2V1 = datastreamVersion(dsId2, true, MANAGED, "text/plain", "goodbye", null);

        final var ds2V2 = datastreamVersion(dsId2, false, MANAGED, "text/plain", "fedora", null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1V1, ds2V1)),
                objectVersionReference(pid, false, List.of(ds2V2))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var ocflObjectId = addPrefix(pid);
        final var session = sessionFactory.newSession(ocflObjectId);

        verifyObjectRdf(contentToString(session, ocflObjectId));
        verifyObjectHeaders(session, ocflObjectId);

        verifyBinary(contentToString(session, ocflObjectId, dsId1), ds1V1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1V1);
        verifyDescRdf(session, ocflObjectId, dsId1, ds1V1);
        verifyDescHeaders(session, ocflObjectId, dsId1);

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId2, "v1"), ds2V1);
        verifyHeaders(session, ocflObjectId, dsId2, ds2V1, "v1");
        verifyDescRdf(session, ocflObjectId, dsId2, ds2V1, "v1");
        verifyDescHeaders(session, ocflObjectId, dsId2, "v1");

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId2, "v2"), ds2V2);
        verifyHeaders(session, ocflObjectId, dsId2, ds2V2, "v2");
        verifyDescRdf(session, ocflObjectId, dsId2, ds2V2, "v2");
        verifyDescHeaders(session, ocflObjectId, dsId2, "v2");
    }

    @Test
    public void updateFilenameFromRelsInt() throws IOException {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, false, false);

        final var pid = "obj2";
        final var dsId1 = "ds3";

        final var ds1V1 = datastreamVersion(dsId1, true, MANAGED, "application/xml", "<h1>hello</h1>", null);
        final var relsIntV1 = datastreamVersion(RELS_INT, true, MANAGED, "application/rdf+xml",
                "<rdf:RDF xmlns:fedora-model=\"info:fedora/fedora-system:def/model#\"" +
                        " xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
                        "\t<rdf:Description rdf:about=\"info:fedora/obj2/ds3\">\n" +
                        "\t\t<fedora-model:downloadFilename>example.xml</fedora-model:downloadFilename>\n" +
                        "\t</rdf:Description>\n" +
                        "</rdf:RDF>", null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1V1)),
                objectVersionReference(pid, false, List.of(relsIntV1))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var ocflObjectId = addPrefix(pid);
        final var session = sessionFactory.newSession(ocflObjectId);

        verifyObjectRdf(contentToString(session, ocflObjectId));
        verifyObjectHeaders(session, ocflObjectId);

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId1, "v1"), ds1V1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1V1, "v1");
        verifyDescRdf(session, ocflObjectId, dsId1, ds1V1, "v1");
        verifyDescHeaders(session, ocflObjectId, dsId1, "v1");

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId1, "v2"), ds1V1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1V1, "v2", "example.xml");
        verifyDescRdf(session, ocflObjectId, dsId1, ds1V1, "v2");
        verifyDescHeaders(session, ocflObjectId, dsId1, "v2");
    }

    @Test
    public void filenameRemovedFromRelsInt() throws IOException {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, false, false);

        final var pid = "obj2";
        final var dsId1 = "ds3";

        final var ds1V1 = datastreamVersion(dsId1, true, MANAGED, "application/xml", "<h1>hello</h1>", null);
        final var relsIntV1 = datastreamVersion(RELS_INT, true, MANAGED, "application/rdf+xml",
                "<rdf:RDF xmlns:fedora-model=\"info:fedora/fedora-system:def/model#\"" +
                        " xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
                        "\t<rdf:Description rdf:about=\"info:fedora/obj2/ds3\">\n" +
                        "\t\t<fedora-model:downloadFilename>example.xml</fedora-model:downloadFilename>\n" +
                        "\t</rdf:Description>\n" +
                        "</rdf:RDF>", null);
        final var relsIntV2 = datastreamVersion(RELS_INT, false, MANAGED, "application/rdf+xml",
                "<rdf:RDF xmlns:fedora-model=\"info:fedora/fedora-system:def/model#\"" +
                        " xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
                        "</rdf:RDF>", null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1V1)),
                objectVersionReference(pid, false, List.of(relsIntV1)),
                objectVersionReference(pid, false, List.of(relsIntV2))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var ocflObjectId = addPrefix(pid);
        final var session = sessionFactory.newSession(ocflObjectId);

        verifyObjectRdf(contentToString(session, ocflObjectId));
        verifyObjectHeaders(session, ocflObjectId);

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId1, "v1"), ds1V1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1V1, "v1");
        verifyDescRdf(session, ocflObjectId, dsId1, ds1V1, "v1");
        verifyDescHeaders(session, ocflObjectId, dsId1, "v1");

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId1, "v2"), ds1V1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1V1, "v2", "example.xml");
        verifyDescRdf(session, ocflObjectId, dsId1, ds1V1, "v2");
        verifyDescHeaders(session, ocflObjectId, dsId1, "v2");

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId1, "v3"), ds1V1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1V1, "v3", "ds3-label");
        verifyDescRdf(session, ocflObjectId, dsId1, ds1V1, "v3");
        verifyDescHeaders(session, ocflObjectId, dsId1, "v3");
    }

    @Test
    public void filenameRemovedFromRelsIntAndLabelChanged() throws IOException {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, true, false);

        final var pid = "obj2";
        final var dsId1 = "ds3";

        final var ds1V1 = datastreamVersion(dsId1, true, MANAGED, "application/xml", "<h1>hello</h1>", null);
        final var relsIntV1 = datastreamVersion(RELS_INT, true, MANAGED, "application/rdf+xml",
                "<rdf:RDF xmlns:fedora-model=\"info:fedora/fedora-system:def/model#\"" +
                        " xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
                        "\t<rdf:Description rdf:about=\"info:fedora/obj2/ds3\">\n" +
                        "\t\t<fedora-model:downloadFilename>example.xml</fedora-model:downloadFilename>\n" +
                        "\t</rdf:Description>\n" +
                        "</rdf:RDF>", null);

        final var ds1V2 = datastreamVersion(dsId1, false, MANAGED, "application/xml", "<h1>hello</h1>",
                DS_ACTIVE, null, "test");
        final var relsIntV2 = datastreamVersion(RELS_INT, false, MANAGED, "application/rdf+xml",
                "<rdf:RDF xmlns:fedora-model=\"info:fedora/fedora-system:def/model#\"" +
                        " xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
                        "</rdf:RDF>", null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1V1)),
                objectVersionReference(pid, false, List.of(relsIntV1)),
                objectVersionReference(pid, false, List.of(relsIntV2, ds1V2))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var ocflObjectId = addPrefix(pid);
        final var session = sessionFactory.newSession(ocflObjectId);

        verifyObjectRdf(contentToString(session, ocflObjectId));
        verifyObjectHeaders(session, ocflObjectId);

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId1, "v1"), ds1V1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1V1, "v1", "ds3-label.xml");
        verifyDescRdf(session, ocflObjectId, dsId1, ds1V1, "v1");
        verifyDescHeaders(session, ocflObjectId, dsId1, "v1");

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId1, "v2"), ds1V1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1V1, "v2", "example.xml");
        verifyDescRdf(session, ocflObjectId, dsId1, ds1V1, "v2");
        verifyDescHeaders(session, ocflObjectId, dsId1, "v2");

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId1, "v3"), ds1V1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1V2, "v3", "test.xml");
        verifyDescRdf(session, ocflObjectId, dsId1, ds1V2, "v3");
        verifyDescHeaders(session, ocflObjectId, dsId1, "v3");
    }

    @Test
    public void addRelsTriples() throws IOException {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, false, false);

        final var pid = "obj2";
        final var dsId1 = "ds3";
        final var dsId2 = "ds4";

        final var ds1V1 = datastreamVersion(dsId1, true, MANAGED, "application/xml", "<h1>hello</h1>", null);
        final var ds2V1 = datastreamVersion(dsId2, true, MANAGED, "text/plain", "goodbye", null);
        final var relsExtV1 = datastreamVersion(RELS_EXT, true, MANAGED, "application/rdf+xml",
                "<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
                        "\t<rdf:Description rdf:about=\"info:fedora/obj2\">\n" +
                        "\t\t<hasModel xmlns=\"info:fedora/fedora-system:def/model#\"" +
                        " rdf:resource=\"info:fedora/TestObject\"></hasModel>\n" +
                        "\t\t<isMemberOf xmlns=\"info:fedora/fedora-system:def/relations-external#\"" +
                        " rdf:resource=\"info:fedora/obj1\"></isMemberOf>\n" +
                        "\t</rdf:Description>\n" +
                        "</rdf:RDF>", null);
        final var relsIntV1 = datastreamVersion(RELS_INT, true, MANAGED, "application/rdf+xml",
                "<rdf:RDF xmlns:fedora-model=\"info:fedora/fedora-system:def/model#\"" +
                        " xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"" +
                        " xmlns:example=\"http://example.com/#\">\n" +
                        "\t<rdf:Description rdf:about=\"info:fedora/obj2/ds3\">\n" +
                        "\t\t<example:animal>cat</example:animal>\n" +
                        "\t</rdf:Description>\n" +
                        "\t<rdf:Description rdf:about=\"info:fedora/obj2/ds4\">\n" +
                        "\t\t<example:animal>dog</example:animal>\n" +
                        "\t</rdf:Description>\n" +
                        "</rdf:RDF>", null);

        final var ds2V2 = datastreamVersion(dsId2, false, MANAGED, "text/plain", "fedora", null);
        final var relsExtV2 = datastreamVersion(RELS_EXT, false, MANAGED, "application/rdf+xml",
                "<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
                        "\t<rdf:Description rdf:about=\"info:fedora/obj2\">\n" +
                        "\t\t<hasModel xmlns=\"info:fedora/fedora-system:def/model#\"" +
                        " rdf:resource=\"info:fedora/ExampleObject\"></hasModel>\n" +
                        "\t\t<isMemberOf xmlns=\"info:fedora/fedora-system:def/relations-external#\"" +
                        " rdf:resource=\"info:fedora/obj1\"></isMemberOf>\n" +
                        "\t</rdf:Description>\n" +
                        "</rdf:RDF>", null);
        final var relsIntV2 = datastreamVersion(RELS_INT, false, MANAGED, "application/rdf+xml",
                "<rdf:RDF xmlns:fedora-model=\"info:fedora/fedora-system:def/model#\"" +
                        " xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"" +
                        " xmlns:example=\"http://example.com/#\">\n" +
                        "\t<rdf:Description rdf:about=\"info:fedora/obj2/ds3\">\n" +
                        "\t\t<example:animal>cat</example:animal>\n" +
                        "\t</rdf:Description>\n" +
                        "\t<rdf:Description rdf:about=\"info:fedora/obj2/ds4\">\n" +
                        "\t\t<example:animal>frog</example:animal>\n" +
                        "\t</rdf:Description>\n" +
                        "</rdf:RDF>", null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1V1, ds2V1, relsExtV1, relsIntV1)),
                objectVersionReference(pid, false, List.of(ds2V2, relsExtV2)),
                objectVersionReference(pid, false, List.of(relsIntV2))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var ocflObjectId = addPrefix(pid);
        final var session = sessionFactory.newSession(ocflObjectId);

        final var objectRdfV1 = contentVersionToString(session, ocflObjectId, "v1");
        verifyObjectRdf(objectRdfV1);
        assertThat(objectRdfV1, allOf(containsString("TestObject"), containsString("obj1")));
        verifyObjectHeaders(session, ocflObjectId);

        verifyBinary(contentToString(session, ocflObjectId, dsId1), ds1V1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1V1);
        final var ds1V1Rdf = verifyDescRdf(session, ocflObjectId, dsId1, ds1V1);
        assertThat(ds1V1Rdf, allOf(containsString("cat"), not(containsString("dog"))));
        verifyDescHeaders(session, ocflObjectId, dsId1);

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId2, "v1"), ds2V1);
        verifyHeaders(session, ocflObjectId, dsId2, ds2V1, "v1");
        final var ds2V1Rdf = verifyDescRdf(session, ocflObjectId, dsId2, ds2V1, "v1");
        assertThat(ds2V1Rdf, allOf(containsString("dog"), not(containsString("cat"))));
        verifyDescHeaders(session, ocflObjectId, dsId2, "v1");

        final var objectRdfV2 = contentToString(session, ocflObjectId);
        verifyObjectRdf(objectRdfV2);
        assertThat(objectRdfV2, allOf(containsString("ExampleObject"),
                containsString("obj1"), not(containsString("TestObject"))));

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId2, "v2"), ds2V2);
        verifyHeaders(session, ocflObjectId, dsId2, ds2V2, "v2");
        final var ds2V2Rdf = verifyDescRdf(session, ocflObjectId, dsId2, ds2V2, "v2");
        assertThat(ds2V2Rdf, allOf(containsString("dog"), not(containsString("cat"))));
        verifyDescHeaders(session, ocflObjectId, dsId2, "v2");

        final var ds2V3Rdf = verifyDescRdf(session, ocflObjectId, dsId2, ds2V2, "v3");
        assertThat(ds2V3Rdf, allOf(containsString("frog"),
                not(containsString("cat")), not(containsString("dog"))));
    }

    @Test
    public void processObjectMultipleVersionsWithDeletedDsF6Format() throws IOException {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, false, false);

        final var pid = "obj2";
        final var dsId1 = "ds3";
        final var dsId2 = "ds4";

        final var ds1V1 = datastreamVersion(dsId1, true, MANAGED, "application/xml", "<h1>hello</h1>",
                DS_INACTIVE, null, dsId1 + "-label");
        final var ds2V1 = datastreamVersion(dsId2, true, MANAGED, "text/plain", "goodbye",
                DS_DELETED, null, dsId2 + "-label");

        final var ds2V2 = datastreamVersion(dsId2, false, MANAGED, "text/plain", "fedora",
                DS_DELETED, null, dsId2 + "-label");

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1V1, ds2V1)),
                objectVersionReference(pid, false, List.of(ds2V2))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var ocflObjectId = addPrefix(pid);
        final var session = sessionFactory.newSession(ocflObjectId);

        verifyObjectRdf(contentToString(session, ocflObjectId));
        verifyObjectHeaders(session, ocflObjectId);

        verifyBinary(contentToString(session, ocflObjectId, dsId1), ds1V1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1V1);
        verifyDescRdf(session, ocflObjectId, dsId1, ds1V1);
        verifyDescHeaders(session, ocflObjectId, dsId1);

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId2, "v1"), ds2V1);
        verifyHeaders(session, ocflObjectId, dsId2, ds2V1, "v1");
        verifyDescRdf(session, ocflObjectId, dsId2, ds2V1, "v1");
        verifyDescHeaders(session, ocflObjectId, dsId2, "v1");

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId2, "v2"), ds2V2);
        verifyHeaders(session, ocflObjectId, dsId2, ds2V2, "v2");
        verifyDescRdf(session, ocflObjectId, dsId2, ds2V2, "v2");
        verifyDescHeaders(session, ocflObjectId, dsId2, "v2");

        verifyResourceDeleted(session, resourceId(ocflObjectId, dsId2));
        verifyResourceDeleted(session, metadataId(ocflObjectId, dsId2));
    }

    @Test
    public void processObjectMultipleVersionsAndDeleteInactiveF6Format() throws IOException {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, false, true);

        final var pid = "obj2";
        final var dsId1 = "ds3";
        final var dsId2 = "ds4";

        final var ds1V1 = datastreamVersion(dsId1, true, MANAGED, "application/xml", "<h1>hello</h1>",
                DS_INACTIVE, null, dsId1 + "-label");
        final var ds2V1 = datastreamVersion(dsId2, true, MANAGED, "text/plain", "goodbye",
                DS_DELETED, null, dsId2 + "-label");

        final var ds2V2 = datastreamVersion(dsId2, false, MANAGED, "text/plain", "fedora",
                DS_DELETED, null, dsId2 + "-label");

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1V1, ds2V1)),
                objectVersionReference(pid, false, List.of(ds2V2))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var ocflObjectId = addPrefix(pid);
        final var session = sessionFactory.newSession(ocflObjectId);

        verifyObjectRdf(contentToString(session, ocflObjectId));
        verifyObjectHeaders(session, ocflObjectId);

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId1, "v1"), ds1V1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1V1, "v1");
        verifyDescRdf(session, ocflObjectId, dsId1, ds1V1, "v1");
        verifyDescHeaders(session, ocflObjectId, dsId1, "v1");

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId2, "v1"), ds2V1);
        verifyHeaders(session, ocflObjectId, dsId2, ds2V1, "v1");
        verifyDescRdf(session, ocflObjectId, dsId2, ds2V1, "v1");
        verifyDescHeaders(session, ocflObjectId, dsId2, "v1");

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId2, "v2"), ds2V2);
        verifyHeaders(session, ocflObjectId, dsId2, ds2V2, "v2");
        verifyDescRdf(session, ocflObjectId, dsId2, ds2V2, "v2");
        verifyDescHeaders(session, ocflObjectId, dsId2, "v2");

        verifyResourceDeleted(session, resourceId(ocflObjectId, dsId1));
        verifyResourceDeleted(session, metadataId(ocflObjectId, dsId1));
        verifyResourceDeleted(session, resourceId(ocflObjectId, dsId2));
        verifyResourceDeleted(session, metadataId(ocflObjectId, dsId2));
    }

    @Test
    public void processObjectMultipleVersionsAndObjectDeletedF6Format() throws IOException {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, false, false);

        final var pid = "obj2";
        final var dsId1 = "ds3";
        final var dsId2 = "ds4";

        final var ds1V1 = datastreamVersion(dsId1, true, MANAGED, "application/xml", "<h1>hello</h1>", null);
        final var ds2V1 = datastreamVersion(dsId2, true, MANAGED, "text/plain", "goodbye", null);

        final var ds2V2 = datastreamVersion(dsId2, false, MANAGED, "text/plain", "fedora", null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, OBJ_DELETED, List.of(ds1V1, ds2V1)),
                objectVersionReference(pid, false, OBJ_DELETED, List.of(ds2V2))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var ocflObjectId = addPrefix(pid);
        final var session = sessionFactory.newSession(ocflObjectId);

        verifyObjectRdf(contentVersionToString(session, ocflObjectId, "v1"));
        verifyObjectHeaders(session, ocflObjectId, "v1");

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId1, "v1"), ds1V1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1V1, "v1");
        verifyDescRdf(session, ocflObjectId, dsId1, ds1V1, "v1");
        verifyDescHeaders(session, ocflObjectId, dsId1, "v1");

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId2, "v1"), ds2V1);
        verifyHeaders(session, ocflObjectId, dsId2, ds2V1, "v1");
        verifyDescRdf(session, ocflObjectId, dsId2, ds2V1, "v1");
        verifyDescHeaders(session, ocflObjectId, dsId2, "v1");

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId2, "v2"), ds2V2);
        verifyHeaders(session, ocflObjectId, dsId2, ds2V2, "v2");
        verifyDescRdf(session, ocflObjectId, dsId2, ds2V2, "v2");
        verifyDescHeaders(session, ocflObjectId, dsId2, "v2");

        verifyResourceDeleted(session, ocflObjectId);
        verifyResourceDeleted(session, resourceId(ocflObjectId, dsId1));
        verifyResourceDeleted(session, metadataId(ocflObjectId, dsId1));
        verifyResourceDeleted(session, resourceId(ocflObjectId, dsId2));
        verifyResourceDeleted(session, metadataId(ocflObjectId, dsId2));
    }

    @Test
    public void processObjectMultipleVersionsAndObjectInactiveDeletedF6Format() throws IOException {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, false, true);

        final var pid = "obj2";
        final var dsId1 = "ds3";
        final var dsId2 = "ds4";

        final var ds1V1 = datastreamVersion(dsId1, true, MANAGED, "application/xml", "<h1>hello</h1>", null);
        final var ds2V1 = datastreamVersion(dsId2, true, MANAGED, "text/plain", "goodbye", null);

        final var ds2V2 = datastreamVersion(dsId2, false, MANAGED, "text/plain", "fedora", null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, OBJ_INACTIVE, List.of(ds1V1, ds2V1)),
                objectVersionReference(pid, false, OBJ_INACTIVE, List.of(ds2V2))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var ocflObjectId = addPrefix(pid);
        final var session = sessionFactory.newSession(ocflObjectId);

        verifyObjectRdf(contentVersionToString(session, ocflObjectId, "v1"));
        verifyObjectHeaders(session, ocflObjectId, "v1");

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId1, "v1"), ds1V1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1V1, "v1");
        verifyDescRdf(session, ocflObjectId, dsId1, ds1V1, "v1");
        verifyDescHeaders(session, ocflObjectId, dsId1, "v1");

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId2, "v1"), ds2V1);
        verifyHeaders(session, ocflObjectId, dsId2, ds2V1, "v1");
        verifyDescRdf(session, ocflObjectId, dsId2, ds2V1, "v1");
        verifyDescHeaders(session, ocflObjectId, dsId2, "v1");

        verifyBinary(contentVersionToString(session, ocflObjectId, dsId2, "v2"), ds2V2);
        verifyHeaders(session, ocflObjectId, dsId2, ds2V2, "v2");
        verifyDescRdf(session, ocflObjectId, dsId2, ds2V2, "v2");
        verifyDescHeaders(session, ocflObjectId, dsId2, "v2");

        verifyResourceDeleted(session, ocflObjectId);
        verifyResourceDeleted(session, resourceId(ocflObjectId, dsId1));
        verifyResourceDeleted(session, metadataId(ocflObjectId, dsId1));
        verifyResourceDeleted(session, resourceId(ocflObjectId, dsId2));
        verifyResourceDeleted(session, metadataId(ocflObjectId, dsId2));
    }

    @Test
    public void processObjectMultipleVersionsPlainFormat() throws IOException {
        final var handler = createHandler(MigrationType.PLAIN_OCFL, false, false);

        final var pid = "obj2";
        final var dsId1 = "ds3";
        final var dsId2 = "ds4";

        final var ds1V1 = datastreamVersion(dsId1, true, MANAGED, "application/xml", "<h1>hello</h1>", null);
        final var ds2V1 = datastreamVersion(dsId2, true, MANAGED, "text/plain", "goodbye", null);

        final var ds2V2 = datastreamVersion(dsId2, false, MANAGED, "text/plain", "fedora", null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1V1, ds2V1)),
                objectVersionReference(pid, false, List.of(ds2V2))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var rootResourceId = addPrefix(pid);

        verifyFcrepoNotExists(rootResourceId);

        verifyObjectRdf(rawContentToString(rootResourceId, PersistencePaths.rdfResource(rootResourceId, rootResourceId)
                .getContentFilePath()));

        verifyBinary(rawContentVersionToString(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId1)).getContentFilePath(),
                "v1"), ds1V1);
        verifyPlainDescRdf(rawContentVersionToString(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId1)).getContentFilePath(),
                "v1"), ds1V1);

        verifyBinary(rawContentVersionToString(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId2)).getContentFilePath(),
                "v1"), ds2V1);
        verifyPlainDescRdf(rawContentVersionToString(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId2)).getContentFilePath(),
                "v1"), ds2V1);

        verifyBinary(rawContentVersionToString(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId2)).getContentFilePath(),
                "v2"), ds2V2);
        verifyPlainDescRdf(rawContentVersionToString(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId2)).getContentFilePath(),
                "v2"), ds2V2);
    }

    @Test
    public void processObjectMultipleVersionsWithDeletedDsPlainFormat() throws IOException {
        final var handler = createHandler(MigrationType.PLAIN_OCFL, false, false);

        final var pid = "obj2";
        final var dsId1 = "ds3";
        final var dsId2 = "ds4";

        final var ds1V1 = datastreamVersion(dsId1, true, MANAGED, "application/xml", "<h1>hello</h1>", null);
        final var ds2V1 = datastreamVersion(dsId2, true, MANAGED, "text/plain", "goodbye",
                DS_DELETED, null, dsId2 + "-label");

        final var ds2V2 = datastreamVersion(dsId2, false, MANAGED, "text/plain", "fedora",
                DS_DELETED, null, dsId2 + "-label");

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1V1, ds2V1)),
                objectVersionReference(pid, false, List.of(ds2V2))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var rootResourceId = addPrefix(pid);

        verifyFcrepoNotExists(rootResourceId);

        verifyObjectRdf(rawContentToString(rootResourceId, PersistencePaths.rdfResource(rootResourceId, rootResourceId)
                .getContentFilePath()));

        verifyBinary(rawContentVersionToString(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId1)).getContentFilePath(),
                "v1"), ds1V1);
        verifyPlainDescRdf(rawContentVersionToString(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId1)).getContentFilePath(),
                "v1"), ds1V1);

        verifyBinary(rawContentVersionToString(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId2)).getContentFilePath(),
                "v1"), ds2V1);
        verifyPlainDescRdf(rawContentVersionToString(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId2)).getContentFilePath(),
                "v1"), ds2V1);

        verifyBinary(rawContentVersionToString(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId2)).getContentFilePath(),
                "v2"), ds2V2);
        verifyPlainDescRdf(rawContentVersionToString(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId2)).getContentFilePath(),
                "v2"), ds2V2);

        rawVerifyDoesNotExist(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId2))
                        .getContentFilePath());
        rawVerifyDoesNotExist(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId2)).getContentFilePath());
    }

    @Test
    public void processObjectMultipleVersionsWithInactiveDeletedObjectPlainFormat() throws IOException {
        final var handler = createHandler(MigrationType.PLAIN_OCFL, false, true);

        final var pid = "obj2";
        final var dsId1 = "ds3";
        final var dsId2 = "ds4";

        final var ds1V1 = datastreamVersion(dsId1, true, MANAGED, "application/xml", "<h1>hello</h1>", null);
        final var ds2V1 = datastreamVersion(dsId2, true, MANAGED, "text/plain", "goodbye", null);

        final var ds2V2 = datastreamVersion(dsId2, false, MANAGED, "text/plain", "fedora", null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, OBJ_INACTIVE, List.of(ds1V1, ds2V1)),
                objectVersionReference(pid, false, OBJ_INACTIVE, List.of(ds2V2))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var rootResourceId = addPrefix(pid);

        verifyFcrepoNotExists(rootResourceId);

        verifyObjectRdf(rawContentVersionToString(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, rootResourceId).getContentFilePath(), "v1"));

        verifyBinary(rawContentVersionToString(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId1)).getContentFilePath(),
                "v1"), ds1V1);
        verifyPlainDescRdf(rawContentVersionToString(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId1)).getContentFilePath(),
                "v1"), ds1V1);

        verifyBinary(rawContentVersionToString(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId2)).getContentFilePath(),
                "v1"), ds2V1);
        verifyPlainDescRdf(rawContentVersionToString(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId2)).getContentFilePath(),
                "v1"), ds2V1);

        verifyBinary(rawContentVersionToString(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId2)).getContentFilePath(),
                "v2"), ds2V2);
        verifyPlainDescRdf(rawContentVersionToString(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId2)).getContentFilePath(),
                "v2"), ds2V2);

        rawVerifyDoesNotExist(rootResourceId, PersistencePaths.rdfResource(rootResourceId, rootResourceId)
                .getContentFilePath());

        rawVerifyDoesNotExist(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId1))
                        .getContentFilePath());
        rawVerifyDoesNotExist(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId1)).getContentFilePath());

        rawVerifyDoesNotExist(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId2))
                        .getContentFilePath());
        rawVerifyDoesNotExist(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId2)).getContentFilePath());
    }

    @Test
    public void processObjectSingleVersionF6FormatWithExtensions() throws IOException {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, true, false);

        final var pid = "obj1";
        final var dsId1 = "ds1";
        final var dsId2 = "ds2";
        final var dsId3 = "ds3";

        final var ds1 = datastreamVersion(dsId1, true, MANAGED, "text/plain", "text", null);
        final var ds2 = datastreamVersion(dsId2, true, MANAGED, "application/rdf+xml", "xml", null);
        final var ds3 = datastreamVersion(dsId3, true, MANAGED, "image/jpeg", "image", null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1, ds2, ds3))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var ocflObjectId = addPrefix(pid);
        final var session = sessionFactory.newSession(ocflObjectId);

        verifyObjectRdf(contentToString(session, ocflObjectId));
        verifyObjectHeaders(session, ocflObjectId);

        verifyBinary(contentToString(session, ocflObjectId, dsId1), ds1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1, "v1", "ds1-label.txt");
        verifyDescRdf(session, ocflObjectId, dsId1, ds1);
        verifyDescHeaders(session, ocflObjectId, dsId1);

        verifyBinary(contentToString(session, ocflObjectId, dsId2), ds2);
        verifyHeaders(session, ocflObjectId, dsId2, ds2, "v1", "ds2-label.rdf");
        verifyDescRdf(session, ocflObjectId, dsId2, ds2);
        verifyDescHeaders(session, ocflObjectId, dsId2);

        verifyBinary(contentToString(session, ocflObjectId, dsId3), ds3);
        verifyHeaders(session, ocflObjectId, dsId3, ds3, "v1", "ds3-label.jpg");
        verifyDescRdf(session, ocflObjectId, dsId3, ds3);
        verifyDescHeaders(session, ocflObjectId, dsId3);
    }

    @Test
    public void processObjectSingleVersionPlainFormatWithExtensions() throws IOException {
        final var handler = createHandler(MigrationType.PLAIN_OCFL, true, false);

        final var pid = "obj1";
        final var dsId1 = "ds1";
        final var dsId2 = "ds2";
        final var dsId3 = "ds3";

        final var ds1 = datastreamVersion(dsId1, true, MANAGED, "text/plain", "text", null);
        final var ds2 = datastreamVersion(dsId2, true, MANAGED, "application/rdf+xml", "xml", null);
        final var ds3 = datastreamVersion(dsId3, true, MANAGED, "image/jpeg", "image", null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1, ds2, ds3))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var rootResourceId = addPrefix(pid);

        verifyFcrepoNotExists(rootResourceId);

        verifyObjectRdf(rawContentToString(rootResourceId, PersistencePaths.rdfResource(rootResourceId, rootResourceId)
                .getContentFilePath()));

        verifyBinary(rawContentToString(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId1))
                        .getContentFilePath()),
                ds1);
        verifyPlainDescRdf(rawContentToString(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId1))
                        .getContentFilePath()),
                ds1);

        verifyBinary(rawContentToString(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId2))
                        .getContentFilePath()),
                ds2);
        verifyPlainDescRdf(rawContentToString(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId2))
                        .getContentFilePath()),
                ds2);

        verifyBinary(rawContentToString(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId3))
                        .getContentFilePath()),
                ds3);
        verifyPlainDescRdf(rawContentToString(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId3))
                        .getContentFilePath()),
                ds3);
    }

    @Test
    public void processObjectSingleVersionF6FormatWithExternalBinary() throws IOException {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, false, false);

        final var pid = "obj1";
        final var dsId1 = "ds1";
        final var dsId2 = "ds2";
        final var dsId3 = "ds3";

        final var ds1 = datastreamVersion(dsId1, true, MANAGED, "text/plain", "hello", null);
        final var ds2 = datastreamVersion(dsId2, true, PROXY, "text/plain", "", "https://external");
        final var ds3 = datastreamVersion(dsId3, true, REDIRECT, "text/plain", "", "https://redirect");

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1, ds2, ds3))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var ocflObjectId = addPrefix(pid);
        final var session = sessionFactory.newSession(ocflObjectId);

        verifyObjectRdf(contentToString(session, ocflObjectId));
        verifyObjectHeaders(session, ocflObjectId);

        verifyBinary(contentToString(session, ocflObjectId, dsId1), ds1);
        verifyHeaders(session, ocflObjectId, dsId1, ds1);
        verifyDescRdf(session, ocflObjectId, dsId1, ds1);
        verifyDescHeaders(session, ocflObjectId, dsId1);

        verifyContentNotExists(session, resourceId(ocflObjectId, dsId2));
        verifyHeaders(session, ocflObjectId, dsId2, ds2);
        verifyDescRdf(session, ocflObjectId, dsId2, ds2);
        verifyDescHeaders(session, ocflObjectId, dsId2);

        verifyContentNotExists(session, resourceId(ocflObjectId, dsId3));
        verifyHeaders(session, ocflObjectId, dsId3, ds3);
        verifyDescRdf(session, ocflObjectId, dsId3, ds3);
        verifyDescHeaders(session, ocflObjectId, dsId3);
    }

    @Test
    public void processObjectSingleVersionPlainFormatWithExternalBinary() throws IOException {
        final var handler = createHandler(MigrationType.PLAIN_OCFL, false, false);

        final var pid = "obj1";
        final var dsId1 = "ds1";
        final var dsId2 = "ds2";
        final var dsId3 = "ds3";

        final var ds1 = datastreamVersion(dsId1, true, MANAGED, "text/plain", "hello", null);
        final var ds2 = datastreamVersion(dsId2, true, PROXY, "text/plain", "", "https://external");
        final var ds3 = datastreamVersion(dsId3, true, REDIRECT, "text/plain", "", "https://redirect");

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1, ds2, ds3))
        ), new DefaultObjectInfo(pid, pid, Files.createTempFile(tempDir.getRoot().toPath(), "foxml", "xml")));

        final var rootResourceId = addPrefix(pid);

        verifyFcrepoNotExists(rootResourceId);

        verifyObjectRdf(rawContentToString(rootResourceId, PersistencePaths.rdfResource(rootResourceId, rootResourceId)
                .getContentFilePath()));

        verifyBinary(rawContentToString(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId1))
                        .getContentFilePath()),
                ds1);
        verifyPlainDescRdf(rawContentToString(rootResourceId,
                PersistencePaths.rdfResource(rootResourceId, metadataId(rootResourceId, dsId1)).getContentFilePath()),
                ds1);

        verifyBinary(rawContentToString(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId2))
                        .getContentFilePath()),
                ds2);

        verifyBinary(rawContentToString(rootResourceId,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(rootResourceId, dsId3))
                        .getContentFilePath()),
                ds3);
    }

    private void verifyBinary(final String content, final DatastreamVersion datastreamVersion) {
        try {
            if ("RE".contains(datastreamVersion.getDatastreamInfo().getControlGroup())) {
                assertEquals(datastreamVersion.getExternalOrRedirectURL(), content);
            } else {
                assertEquals(IOUtils.toString(datastreamVersion.getContent()), content);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void verifyHeaders(final OcflObjectSession session,
                               final String ocflObjectId,
                               final String dsId,
                               final DatastreamVersion datastreamVersion) {
        verifyHeaders(session, ocflObjectId, dsId, datastreamVersion, null);
    }

    private void verifyHeaders(final OcflObjectSession session,
                               final String ocflObjectId,
                               final String dsId,
                               final DatastreamVersion datastreamVersion,
                               final String versionNumber) {
        verifyHeaders(session, ocflObjectId, dsId, datastreamVersion, versionNumber, dsId + "-label");
    }

    private void verifyHeaders(final OcflObjectSession session,
                               final String ocflObjectId,
                               final String dsId,
                               final DatastreamVersion datastreamVersion,
                               final String versionNumber,
                               final String filename) {
        final var resourceId = resourceId(ocflObjectId, dsId);
        try (final var content = session.readContent(resourceId, versionNumber)) {
            final var headers = content.getHeaders();
            assertEquals(ResourceHeadersVersion.V1_0, headers.getHeadersVersion());
            assertEquals(resourceId, headers.getId());
            assertEquals(ocflObjectId, headers.getParent());
            assertEquals(ocflObjectId, headers.getArchivalGroupId());
            assertEquals(InteractionModel.NON_RDF.getUri(), headers.getInteractionModel());
            assertFalse("not AG", headers.isArchivalGroup());
            assertFalse("not root", headers.isObjectRoot());
            assertFalse("not deleted", headers.isDeleted());
            assertEquals(USER, headers.getCreatedBy());
            assertEquals(USER, headers.getLastModifiedBy());
            assertThat(headers.getLastModifiedDate().toString(), containsString(date));
            assertThat(headers.getMementoCreatedDate().toString(), containsString(date));
            assertThat(headers.getCreatedDate().toString(), containsString(date));
            if (INLINE.equals(datastreamVersion.getDatastreamInfo().getControlGroup())) {
                assertNotEquals(datastreamVersion.getSize(), headers.getContentSize());
            } else {
                assertEquals(datastreamVersion.getSize(), headers.getContentSize());
            }
            assertEquals(datastreamVersion.getMimeType(), headers.getMimeType());
            assertEquals(filename, headers.getFilename());
            assertEquals(DigestUtils.md5Hex(
                    String.valueOf(Instant.parse(datastreamVersion.getCreated()).toEpochMilli())).toUpperCase(),
                    headers.getStateToken());
            if (headers.getExternalHandling() != null) {
                assertEquals(1, headers.getDigests().size());
            } else {
                assertEquals(2, headers.getDigests().size());
            }
            assertEquals(datastreamVersion.getExternalOrRedirectURL(), headers.getExternalUrl());
            if (Objects.equals(REDIRECT, datastreamVersion.getDatastreamInfo().getControlGroup())) {
                assertEquals("redirect", headers.getExternalHandling());
            } else if (Objects.equals(PROXY, datastreamVersion.getDatastreamInfo().getControlGroup())) {
                assertEquals("proxy", headers.getExternalHandling());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void verifyDescHeaders(final OcflObjectSession session,
                                   final String ocflObjectId,
                                   final String dsId) {
        verifyDescHeaders(session, ocflObjectId, dsId, null);
    }

    private void verifyDescHeaders(final OcflObjectSession session,
                                   final String ocflObjectId,
                                   final String dsId,
                                   final String versionNumber) {
        try (final var content = session.readContent(metadataId(ocflObjectId, dsId), versionNumber)) {
            final var headers = content.getHeaders();
            assertEquals(ResourceHeadersVersion.V1_0, headers.getHeadersVersion());
            assertEquals(metadataId(ocflObjectId, dsId), headers.getId());
            assertEquals(resourceId(ocflObjectId, dsId), headers.getParent());
            assertEquals(ocflObjectId, headers.getArchivalGroupId());
            assertEquals(InteractionModel.NON_RDF_DESCRIPTION.getUri(), headers.getInteractionModel());
            assertFalse("not AG", headers.isArchivalGroup());
            assertFalse("not root", headers.isObjectRoot());
            assertFalse("not deleted", headers.isDeleted());
            assertEquals(USER, headers.getCreatedBy());
            assertEquals(USER, headers.getLastModifiedBy());
            assertThat(headers.getLastModifiedDate().toString(), containsString(date));
            assertThat(headers.getMementoCreatedDate().toString(), containsString(date));
            assertThat(headers.getCreatedDate().toString(), containsString(date));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void verifyObjectHeaders(final OcflObjectSession session,
                                     final String ocflObjectId) {
        verifyObjectHeaders(session, ocflObjectId, null);
    }

    private void verifyObjectHeaders(final OcflObjectSession session,
                                     final String ocflObjectId,
                                     final String versionNumber) {
        try (final var content = session.readContent(ocflObjectId, versionNumber)) {
            final var headers = content.getHeaders();
            assertEquals(ResourceHeadersVersion.V1_0, headers.getHeadersVersion());
            assertEquals(ocflObjectId, headers.getId());
            assertEquals(FCREPO_ROOT, headers.getParent());
            assertEquals(InteractionModel.BASIC_CONTAINER.getUri(), headers.getInteractionModel());
            assertTrue("is AG", headers.isArchivalGroup());
            assertTrue("is root", headers.isObjectRoot());
            assertFalse("not deleted", headers.isDeleted());
            assertEquals(USER, headers.getCreatedBy());
            assertEquals(USER, headers.getLastModifiedBy());
            assertThat(headers.getLastModifiedDate().toString(), containsString(date));
            assertThat(headers.getMementoCreatedDate().toString(), containsString(date));
            assertThat(headers.getCreatedDate().toString(), containsString(date));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String verifyDescRdf(final OcflObjectSession session,
                               final String ocflObjectId,
                               final String dsId,
                               final DatastreamVersion datastreamVersion) {
        return verifyDescRdf(session, ocflObjectId, dsId, datastreamVersion, null);
    }

    private String verifyDescRdf(final OcflObjectSession session,
                               final String ocflObjectId,
                               final String dsId,
                               final DatastreamVersion datastreamVersion,
                               final String versionNumber) {
        try (final var content = session.readContent(metadataId(ocflObjectId, dsId), versionNumber)) {
            final var value = IOUtils.toString(content.getContentStream().get());
            assertThat(value, allOf(
                    containsString(datastreamVersion.getLabel()),
                    containsString(datastreamVersion.getFormatUri()),
                    containsString("objState")
            ));
            return value;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void verifyPlainDescRdf(final String content,
                                    final DatastreamVersion datastreamVersion) {
        assertThat(content, allOf(
                containsString(datastreamVersion.getLabel()),
                containsString(datastreamVersion.getFormatUri()),
                containsString(datastreamVersion.getMimeType()),
                containsString(datastreamVersion.getDatastreamInfo().getDatastreamId()),
                containsString("objState"),
                containsString("hasMessageDigest"),
                containsString("lastModified"),
                containsString(date),
                containsString("created")));
    }

    private void verifyObjectRdf(final String content) {
        assertThat(content, allOf(
                containsString("lastModifiedDate"),
                containsString(date),
                containsString("createdDate")
        ));
    }

    private String contentToString(final OcflObjectSession session, final String ocflObjectId) {
        return contentVersionToString(session, ocflObjectId, null);
    }

    private String contentVersionToString(final OcflObjectSession session,
                                          final String ocflObjectId,
                                          final String versionNumber) {
        try (final var content = session.readContent(ocflObjectId, versionNumber)) {
            return IOUtils.toString(content.getContentStream().get());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String contentToString(final OcflObjectSession session, final String ocflObjectId, final String dsId) {
        return contentVersionToString(session, ocflObjectId, dsId, null);
    }

    private String contentVersionToString(final OcflObjectSession session,
                                          final String ocflObjectId,
                                          final String dsId,
                                          final String versionNumber) {
        try (final var content = session.readContent(resourceId(ocflObjectId, dsId), versionNumber)) {
            return IOUtils.toString(content.getContentStream().get());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String rawContentToString(final String ocflObjectId, final String path) {
        try (final var content = ocflRepo.getObject(ObjectVersionId.head(ocflObjectId))
                .getFile(path).getStream()) {
            return IOUtils.toString(content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String rawContentVersionToString(final String ocflObjectId, final String path, final String versionNumber) {
        try (final var content = ocflRepo.getObject(
                ObjectVersionId.version(ocflObjectId, versionNumber)).getFile(path).getStream()) {
            return IOUtils.toString(content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void rawVerifyDoesNotExist(final String ocflObjectId, final String path) {
        assertFalse(String.format("object %s not contain path %s", ocflObjectId, path),
                ocflRepo.describeVersion(ObjectVersionId.head(ocflObjectId)).containsFile(path));
    }

    private ArchiveGroupHandler createHandler(final MigrationType migrationType,
                                              final boolean addExtensions,
                                              final boolean deleteInactive) {
        if (migrationType == MigrationType.PLAIN_OCFL) {
            return new ArchiveGroupHandler(plainSessionFactory, migrationType, addExtensions, deleteInactive,
                    false, USER,"info:fedora/", false);
        } else {
            return new ArchiveGroupHandler(sessionFactory, migrationType, addExtensions, deleteInactive, false, USER,
                    "info:fedora/", false);
        }
    }

    private ObjectVersionReference objectVersionReference(final String pid,
                                                          final boolean isFirst,
                                                          final List<DatastreamVersion> datastreamVersions)
            throws IOException {
        return objectVersionReference(pid, isFirst, OBJ_ACTIVE, datastreamVersions);
    }

    private ObjectVersionReference objectVersionReference(final String pid,
                                                          final boolean isFirst,
                                                          final String state,
                                                          final List<DatastreamVersion> datastreamVersions)
            throws IOException {
        final var mock = Mockito.mock(ObjectVersionReference.class);
        when(mock.isFirstVersion()).thenReturn(isFirst);
        if (isFirst) {
            final var properties = objectProperties(List.of(
                    objectProperty("info:fedora/fedora-system:def/view#lastModifiedDate", Instant.now().toString()),
                    objectProperty("info:fedora/fedora-system:def/model#createdDate", Instant.now().toString()),
                    objectProperty("info:fedora/fedora-system:def/model#state", state)
            ));
            when(mock.getObjectProperties()).thenReturn(properties);
        }
        when(mock.getObject()).thenReturn(null);
        when(mock.listChangedDatastreams()).thenReturn(datastreamVersions);
        when(mock.getVersionDate()).thenReturn(Instant.now().toString());
        return mock;
    }

    private ObjectProperties objectProperties(final List<? extends ObjectProperty> properties) {
        final var mock = Mockito.mock(ObjectProperties.class);
        doReturn(properties).when(mock).listProperties();
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
        return datastreamVersion(datastreamId, isFirst, controlGroup, mimeType,
                content, DS_ACTIVE, externalUrl, datastreamId + "-label");
    }

    private DatastreamVersion datastreamVersion(final String datastreamId,
                                                final boolean isFirst,
                                                final String controlGroup,
                                                final String mimeType,
                                                final String content,
                                                final String state,
                                                final String externalUrl,
                                                final String label) {
        final var mock = Mockito.mock(DatastreamVersion.class);
        final var info = datastreamInfo(datastreamId, controlGroup, state);
        when(mock.getDatastreamInfo()).thenReturn(info);
        when(mock.getMimeType()).thenReturn(mimeType);
        try {
            when(mock.getContent()).thenAnswer((Answer<InputStream>) invocation -> {
                return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        when(mock.isFirstVersionIn(Mockito.isNull())).thenReturn(isFirst);
        when(mock.getCreated()).thenReturn(Instant.now().toString());
        when(mock.getExternalOrRedirectURL()).thenReturn(externalUrl);
        when(mock.getSize()).thenReturn((long) content.length());
        final var contentDigest = contentDigest(content);
        when(mock.getContentDigest()).thenReturn(contentDigest);
        when(mock.getLabel()).thenReturn(label);
        when(mock.getFormatUri()).thenReturn("http://format-id");
        return mock;
    }

    private DatastreamInfo datastreamInfo(final String datastreamId, final String controlGroup, final String state) {
        final var mock = Mockito.mock(DatastreamInfo.class);
        when(mock.getDatastreamId()).thenReturn(datastreamId);
        when(mock.getControlGroup()).thenReturn(controlGroup);
        when(mock.getState()).thenReturn(state);
        return mock;
    }

    private ContentDigest contentDigest(final String content) {
        final var mock = Mockito.mock(ContentDigest.class);
        when(mock.getType()).thenReturn("md5");
        when(mock.getDigest()).thenReturn(DigestUtils.md5Hex(content));
        return mock;
    }

    private String addPrefix(final String pid) {
        return FCREPO_ROOT + pid;
    }

    private String resourceId(final String ocflObjectId, final String dsId) {
        return ocflObjectId + "/" + dsId;
    }

    private String metadataId(final String ocflObjectId, final String dsId) {
        return resourceId(ocflObjectId, dsId) + "/fcr:metadata";
    }

    private void verifyFcrepoNotExists(final String ocflObjectId) {
        final var count = ocflRepo.describeVersion(ObjectVersionId.head(ocflObjectId)).getFiles().stream()
                .map(FileDetails::getPath)
                .filter(file -> file.startsWith(".fcrepo/"))
                .count();
        assertEquals(0, count);
    }

    private void verifyContentNotExists(final OcflObjectSession session, final String ocflObjectId) {
        try (final var content = session.readContent(ocflObjectId)) {
            assertTrue("Content should not exist", content.getContentStream().isEmpty());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void verifyResourceDeleted(final OcflObjectSession session, final String ocflObjectId) {
        final var headers = session.readHeaders(ocflObjectId);
        assertTrue("resource " + ocflObjectId + " should be deleted", headers.isDeleted());
        verifyContentNotExists(session, ocflObjectId);
    }

}
