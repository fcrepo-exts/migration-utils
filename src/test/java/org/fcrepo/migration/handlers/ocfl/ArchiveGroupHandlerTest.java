package org.fcrepo.migration.handlers.ocfl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.model.FileDetails;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedTruncatedNTupleConfig;
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
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
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
                .layoutConfig(new HashedTruncatedNTupleConfig())
                .logicalPathMapper(logicalPathMapper)
                .storage(FileSystemOcflStorage.builder().repositoryRoot(ocflRoot).build())
                .workDir(staging)
                .buildMutable();

        objectMapper = new ObjectMapper()
                .configure(WRITE_DATES_AS_TIMESTAMPS, false)
                .registerModule(new JavaTimeModule())
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);

        sessionFactory = new DefaultOcflObjectSessionFactory(ocflRepo, staging, objectMapper,
                new NoOpCache<>(), CommitType.NEW_VERSION,
                "testing", USER, "info:fedora/fedoraAdmin");

        plainSessionFactory = new PlainOcflObjectSessionFactory(ocflRepo, staging,
                "testing", USER, "info:fedora/fedoraAdmin");

        date = Instant.now().toString().substring(0, 10);
    }

    @Test
    public void processObjectSingleVersionF6Format() {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, false, false);

        final var pid = "obj1";
        final var dsId1 = "ds1";
        final var dsId2 = "ds2";

        final var ds1 = datastreamVersion(dsId1, true, MANAGED, "text/plain", "hello", null);
        final var ds2 = datastreamVersion(dsId2, true, INLINE, "application/xml", "<xml>goodbye</xml>", null);
        when(ds2.getSize()).thenReturn(100L);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1, ds2))
        ));

        final var session = sessionFactory.newSession(resourceId(pid));

        verifyObjectRdf(contentToString(session, pid));
        verifyObjectHeaders(session, pid);

        verifyBinary(contentToString(session, pid, dsId1), ds1);
        verifyHeaders(session, pid, dsId1, ds1);
        verifyDescRdf(session, pid, dsId1, ds1);
        verifyDescHeaders(session, pid, dsId1);

        verifyBinary(contentToString(session, pid, dsId2), ds2);
        verifyHeaders(session, pid, dsId2, ds2);
        verifyDescRdf(session, pid, dsId2, ds2);
        verifyDescHeaders(session, pid, dsId2);
    }

    @Test
    public void processObjectMultipleVersionsF6Format() {
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
        ));

        final var session = sessionFactory.newSession(resourceId(pid));

        verifyObjectRdf(contentToString(session, pid));
        verifyObjectHeaders(session, pid);

        verifyBinary(contentToString(session, pid, dsId1), ds1V1);
        verifyHeaders(session, pid, dsId1, ds1V1);
        verifyDescRdf(session, pid, dsId1, ds1V1);
        verifyDescHeaders(session, pid, dsId1);

        verifyBinary(contentVersionToString(session, pid, dsId2, "v1"), ds2V1);
        verifyHeaders(session, pid, dsId2, ds2V1, "v1");
        verifyDescRdf(session, pid, dsId2, ds2V1, "v1");
        verifyDescHeaders(session, pid, dsId2, "v1");

        verifyBinary(contentVersionToString(session, pid, dsId2, "v2"), ds2V2);
        verifyHeaders(session, pid, dsId2, ds2V2, "v2");
        verifyDescRdf(session, pid, dsId2, ds2V2, "v2");
        verifyDescHeaders(session, pid, dsId2, "v2");
    }

    @Test
    public void processObjectMultipleVersionsWithDeletedDsF6Format() {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, false, false);

        final var pid = "obj2";
        final var dsId1 = "ds3";
        final var dsId2 = "ds4";

        final var ds1V1 = datastreamVersion(dsId1, true, MANAGED, "application/xml", "<h1>hello</h1>",
                DS_INACTIVE, null);
        final var ds2V1 = datastreamVersion(dsId2, true, MANAGED, "text/plain", "goodbye", DS_DELETED, null);

        final var ds2V2 = datastreamVersion(dsId2, false, MANAGED, "text/plain", "fedora", DS_DELETED, null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1V1, ds2V1)),
                objectVersionReference(pid, false, List.of(ds2V2))
        ));

        final var session = sessionFactory.newSession(resourceId(pid));

        verifyObjectRdf(contentToString(session, pid));
        verifyObjectHeaders(session, pid);

        verifyBinary(contentToString(session, pid, dsId1), ds1V1);
        verifyHeaders(session, pid, dsId1, ds1V1);
        verifyDescRdf(session, pid, dsId1, ds1V1);
        verifyDescHeaders(session, pid, dsId1);

        verifyBinary(contentVersionToString(session, pid, dsId2, "v1"), ds2V1);
        verifyHeaders(session, pid, dsId2, ds2V1, "v1");
        verifyDescRdf(session, pid, dsId2, ds2V1, "v1");
        verifyDescHeaders(session, pid, dsId2, "v1");

        verifyBinary(contentVersionToString(session, pid, dsId2, "v2"), ds2V2);
        verifyHeaders(session, pid, dsId2, ds2V2, "v2");
        verifyDescRdf(session, pid, dsId2, ds2V2, "v2");
        verifyDescHeaders(session, pid, dsId2, "v2");

        verifyResourceDeleted(session, resourceId(pid, dsId2));
        verifyResourceDeleted(session, medadataId(pid, dsId2));
    }

    @Test
    public void processObjectMultipleVersionsAndDeleteInactiveF6Format() {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, false, true);

        final var pid = "obj2";
        final var dsId1 = "ds3";
        final var dsId2 = "ds4";

        final var ds1V1 = datastreamVersion(dsId1, true, MANAGED, "application/xml", "<h1>hello</h1>",
                DS_INACTIVE, null);
        final var ds2V1 = datastreamVersion(dsId2, true, MANAGED, "text/plain", "goodbye", DS_DELETED, null);

        final var ds2V2 = datastreamVersion(dsId2, false, MANAGED, "text/plain", "fedora", DS_DELETED, null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1V1, ds2V1)),
                objectVersionReference(pid, false, List.of(ds2V2))
        ));

        final var session = sessionFactory.newSession(resourceId(pid));

        verifyObjectRdf(contentToString(session, pid));
        verifyObjectHeaders(session, pid);

        verifyBinary(contentVersionToString(session, pid, dsId1, "v1"), ds1V1);
        verifyHeaders(session, pid, dsId1, ds1V1, "v1");
        verifyDescRdf(session, pid, dsId1, ds1V1, "v1");
        verifyDescHeaders(session, pid, dsId1, "v1");

        verifyBinary(contentVersionToString(session, pid, dsId2, "v1"), ds2V1);
        verifyHeaders(session, pid, dsId2, ds2V1, "v1");
        verifyDescRdf(session, pid, dsId2, ds2V1, "v1");
        verifyDescHeaders(session, pid, dsId2, "v1");

        verifyBinary(contentVersionToString(session, pid, dsId2, "v2"), ds2V2);
        verifyHeaders(session, pid, dsId2, ds2V2, "v2");
        verifyDescRdf(session, pid, dsId2, ds2V2, "v2");
        verifyDescHeaders(session, pid, dsId2, "v2");

        verifyResourceDeleted(session, resourceId(pid, dsId1));
        verifyResourceDeleted(session, medadataId(pid, dsId1));
        verifyResourceDeleted(session, resourceId(pid, dsId2));
        verifyResourceDeleted(session, medadataId(pid, dsId2));
    }

    @Test
    public void processObjectMultipleVersionsAndObjectDeletedF6Format() {
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
        ));

        final var session = sessionFactory.newSession(resourceId(pid));

        verifyObjectRdf(contentVersionToString(session, pid, "v1"));
        verifyObjectHeaders(session, pid, "v1");

        verifyBinary(contentVersionToString(session, pid, dsId1, "v1"), ds1V1);
        verifyHeaders(session, pid, dsId1, ds1V1, "v1");
        verifyDescRdf(session, pid, dsId1, ds1V1, "v1");
        verifyDescHeaders(session, pid, dsId1, "v1");

        verifyBinary(contentVersionToString(session, pid, dsId2, "v1"), ds2V1);
        verifyHeaders(session, pid, dsId2, ds2V1, "v1");
        verifyDescRdf(session, pid, dsId2, ds2V1, "v1");
        verifyDescHeaders(session, pid, dsId2, "v1");

        verifyBinary(contentVersionToString(session, pid, dsId2, "v2"), ds2V2);
        verifyHeaders(session, pid, dsId2, ds2V2, "v2");
        verifyDescRdf(session, pid, dsId2, ds2V2, "v2");
        verifyDescHeaders(session, pid, dsId2, "v2");

        verifyResourceDeleted(session, resourceId(pid));
        verifyResourceDeleted(session, resourceId(pid, dsId1));
        verifyResourceDeleted(session, medadataId(pid, dsId1));
        verifyResourceDeleted(session, resourceId(pid, dsId2));
        verifyResourceDeleted(session, medadataId(pid, dsId2));
    }

    @Test
    public void processObjectMultipleVersionsAndObjectInactiveDeletedF6Format() {
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
        ));

        final var session = sessionFactory.newSession(resourceId(pid));

        verifyObjectRdf(contentVersionToString(session, pid, "v1"));
        verifyObjectHeaders(session, pid, "v1");

        verifyBinary(contentVersionToString(session, pid, dsId1, "v1"), ds1V1);
        verifyHeaders(session, pid, dsId1, ds1V1, "v1");
        verifyDescRdf(session, pid, dsId1, ds1V1, "v1");
        verifyDescHeaders(session, pid, dsId1, "v1");

        verifyBinary(contentVersionToString(session, pid, dsId2, "v1"), ds2V1);
        verifyHeaders(session, pid, dsId2, ds2V1, "v1");
        verifyDescRdf(session, pid, dsId2, ds2V1, "v1");
        verifyDescHeaders(session, pid, dsId2, "v1");

        verifyBinary(contentVersionToString(session, pid, dsId2, "v2"), ds2V2);
        verifyHeaders(session, pid, dsId2, ds2V2, "v2");
        verifyDescRdf(session, pid, dsId2, ds2V2, "v2");
        verifyDescHeaders(session, pid, dsId2, "v2");

        verifyResourceDeleted(session, resourceId(pid));
        verifyResourceDeleted(session, resourceId(pid, dsId1));
        verifyResourceDeleted(session, medadataId(pid, dsId1));
        verifyResourceDeleted(session, resourceId(pid, dsId2));
        verifyResourceDeleted(session, medadataId(pid, dsId2));
    }

    @Test
    public void processObjectMultipleVersionsPlainFormat() {
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
        ));

        verifyFcrepoNotExists(pid);

        final var rootResourceId = resourceId(pid);

        verifyObjectRdf(rawContentToString(pid, PersistencePaths.rdfResource(rootResourceId, rootResourceId)
                .getContentFilePath()));

        verifyBinary(rawContentVersionToString(pid,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId1)).getContentFilePath(),
                "v1"), ds1V1);
        verifyPlainDescRdf(rawContentVersionToString(pid,
                PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId1)).getContentFilePath(),
                "v1"), ds1V1);

        verifyBinary(rawContentVersionToString(pid,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId2)).getContentFilePath(),
                "v1"), ds2V1);
        verifyPlainDescRdf(rawContentVersionToString(pid,
                PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId2)).getContentFilePath(),
                "v1"), ds2V1);

        verifyBinary(rawContentVersionToString(pid,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId2)).getContentFilePath(),
                "v2"), ds2V2);
        verifyPlainDescRdf(rawContentVersionToString(pid,
                PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId2)).getContentFilePath(),
                "v2"), ds2V2);
    }

    @Test
    public void processObjectMultipleVersionsWithDeletedDsPlainFormat() {
        final var handler = createHandler(MigrationType.PLAIN_OCFL, false, false);

        final var pid = "obj2";
        final var dsId1 = "ds3";
        final var dsId2 = "ds4";

        final var ds1V1 = datastreamVersion(dsId1, true, MANAGED, "application/xml", "<h1>hello</h1>", null);
        final var ds2V1 = datastreamVersion(dsId2, true, MANAGED, "text/plain", "goodbye", DS_DELETED, null);

        final var ds2V2 = datastreamVersion(dsId2, false, MANAGED, "text/plain", "fedora", DS_DELETED, null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1V1, ds2V1)),
                objectVersionReference(pid, false, List.of(ds2V2))
        ));

        verifyFcrepoNotExists(pid);

        final var rootResourceId = resourceId(pid);

        verifyObjectRdf(rawContentToString(pid, PersistencePaths.rdfResource(rootResourceId, rootResourceId)
                .getContentFilePath()));

        verifyBinary(rawContentVersionToString(pid,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId1)).getContentFilePath(),
                "v1"), ds1V1);
        verifyPlainDescRdf(rawContentVersionToString(pid,
                PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId1)).getContentFilePath(),
                "v1"), ds1V1);

        verifyBinary(rawContentVersionToString(pid,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId2)).getContentFilePath(),
                "v1"), ds2V1);
        verifyPlainDescRdf(rawContentVersionToString(pid,
                PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId2)).getContentFilePath(),
                "v1"), ds2V1);

        verifyBinary(rawContentVersionToString(pid,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId2)).getContentFilePath(),
                "v2"), ds2V2);
        verifyPlainDescRdf(rawContentVersionToString(pid,
                PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId2)).getContentFilePath(),
                "v2"), ds2V2);

        rawVerifyDoesNotExist(pid, PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId2))
                .getContentFilePath());
        rawVerifyDoesNotExist(pid, PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId2))
                .getContentFilePath());
    }

    @Test
    public void processObjectMultipleVersionsWithInactiveDeletedObjectPlainFormat() {
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
        ));

        verifyFcrepoNotExists(pid);

        final var rootResourceId = resourceId(pid);

        verifyObjectRdf(rawContentVersionToString(pid, PersistencePaths.rdfResource(rootResourceId, rootResourceId)
                .getContentFilePath(), "v1"));

        verifyBinary(rawContentVersionToString(pid,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId1)).getContentFilePath(),
                "v1"), ds1V1);
        verifyPlainDescRdf(rawContentVersionToString(pid,
                PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId1)).getContentFilePath(),
                "v1"), ds1V1);

        verifyBinary(rawContentVersionToString(pid,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId2)).getContentFilePath(),
                "v1"), ds2V1);
        verifyPlainDescRdf(rawContentVersionToString(pid,
                PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId2)).getContentFilePath(),
                "v1"), ds2V1);

        verifyBinary(rawContentVersionToString(pid,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId2)).getContentFilePath(),
                "v2"), ds2V2);
        verifyPlainDescRdf(rawContentVersionToString(pid,
                PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId2)).getContentFilePath(),
                "v2"), ds2V2);

        rawVerifyDoesNotExist(pid, PersistencePaths.rdfResource(rootResourceId, rootResourceId)
                .getContentFilePath());

        rawVerifyDoesNotExist(pid, PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId1))
                .getContentFilePath());
        rawVerifyDoesNotExist(pid, PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId1))
                .getContentFilePath());

        rawVerifyDoesNotExist(pid, PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId2))
                .getContentFilePath());
        rawVerifyDoesNotExist(pid, PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId2))
                .getContentFilePath());
    }

    @Test
    public void processObjectSingleVersionF6FormatWithExtensions() {
        final var handler = createHandler(MigrationType.FEDORA_OCFL, true, false);

        final var pid = "obj1";
        final var dsId1 = "ds1";
        final var dsId1Ext = "ds1.txt";
        final var dsId2 = "ds2";
        final var dsId2Ext = "ds2.rdf";
        final var dsId3 = "ds3";
        final var dsId3Ext = "ds3.jpg";

        final var ds1 = datastreamVersion(dsId1, true, MANAGED, "text/plain", "text", null);
        final var ds2 = datastreamVersion(dsId2, true, MANAGED, "application/rdf+xml", "xml", null);
        final var ds3 = datastreamVersion(dsId3, true, MANAGED, "image/jpeg", "image", null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1, ds2, ds3))
        ));

        final var session = sessionFactory.newSession(resourceId(pid));

        verifyObjectRdf(contentToString(session, pid));
        verifyObjectHeaders(session, pid);

        verifyBinary(contentToString(session, pid, dsId1Ext), ds1);
        verifyHeaders(session, pid, dsId1Ext, ds1);
        verifyDescRdf(session, pid, dsId1Ext, ds1);
        verifyDescHeaders(session, pid, dsId1Ext);

        verifyBinary(contentToString(session, pid, dsId2Ext), ds2);
        verifyHeaders(session, pid, dsId2Ext, ds2);
        verifyDescRdf(session, pid, dsId2Ext, ds2);
        verifyDescHeaders(session, pid, dsId2Ext);

        verifyBinary(contentToString(session, pid, dsId3Ext), ds3);
        verifyHeaders(session, pid, dsId3Ext, ds3);
        verifyDescRdf(session, pid, dsId3Ext, ds3);
        verifyDescHeaders(session, pid, dsId3Ext);
    }

    @Test
    public void processObjectSingleVersionPlainFormatWithExtensions() {
        final var handler = createHandler(MigrationType.PLAIN_OCFL, true, false);

        final var pid = "obj1";
        final var dsId1 = "ds1";
        final var dsId1Ext = "ds1.txt";
        final var dsId2 = "ds2";
        final var dsId2Ext = "ds2.rdf";
        final var dsId3 = "ds3";
        final var dsId3Ext = "ds3.jpg";

        final var ds1 = datastreamVersion(dsId1, true, MANAGED, "text/plain", "text", null);
        final var ds2 = datastreamVersion(dsId2, true, MANAGED, "application/rdf+xml", "xml", null);
        final var ds3 = datastreamVersion(dsId3, true, MANAGED, "image/jpeg", "image", null);

        handler.processObjectVersions(List.of(
                objectVersionReference(pid, true, List.of(ds1, ds2, ds3))
        ));

        verifyFcrepoNotExists(pid);

        final var rootResourceId = resourceId(pid);

        verifyObjectRdf(rawContentToString(pid, PersistencePaths.rdfResource(rootResourceId, rootResourceId)
                .getContentFilePath()));

        verifyBinary(rawContentToString(pid,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId1Ext)).getContentFilePath()),
                ds1);
        verifyPlainDescRdf(rawContentToString(pid,
                PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId1Ext)).getContentFilePath()),
                ds1);

        verifyBinary(rawContentToString(pid,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId2Ext)).getContentFilePath()),
                ds2);
        verifyPlainDescRdf(rawContentToString(pid,
                PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId2Ext)).getContentFilePath()),
                ds2);

        verifyBinary(rawContentToString(pid,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId3Ext)).getContentFilePath()),
                ds3);
        verifyPlainDescRdf(rawContentToString(pid,
                PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId3Ext)).getContentFilePath()),
                ds3);
    }

    @Test
    public void processObjectSingleVersionF6FormatWithExternalBinary() {
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
        ));

        final var session = sessionFactory.newSession(resourceId(pid));

        verifyObjectRdf(contentToString(session, pid));
        verifyObjectHeaders(session, pid);

        verifyBinary(contentToString(session, pid, dsId1), ds1);
        verifyHeaders(session, pid, dsId1, ds1);
        verifyDescRdf(session, pid, dsId1, ds1);
        verifyDescHeaders(session, pid, dsId1);

        verifyContentNotExists(session, resourceId(pid, dsId2));
        verifyHeaders(session, pid, dsId2, ds2);
        verifyDescRdf(session, pid, dsId2, ds2);
        verifyDescHeaders(session, pid, dsId2);

        verifyContentNotExists(session, resourceId(pid, dsId3));
        verifyHeaders(session, pid, dsId3, ds3);
        verifyDescRdf(session, pid, dsId3, ds3);
        verifyDescHeaders(session, pid, dsId3);
    }

    @Test
    public void processObjectSingleVersionPlainFormatWithExternalBinary() {
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
        ));

        verifyFcrepoNotExists(pid);

        final var rootResourceId = resourceId(pid);

        verifyObjectRdf(rawContentToString(pid, PersistencePaths.rdfResource(rootResourceId, rootResourceId)
                .getContentFilePath()));

        verifyBinary(rawContentToString(pid,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId1)).getContentFilePath()),
                ds1);
        verifyPlainDescRdf(rawContentToString(pid,
                PersistencePaths.rdfResource(rootResourceId, medadataId(pid, dsId1)).getContentFilePath()),
                ds1);

        verifyBinary(rawContentToString(pid,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId2)).getContentFilePath()),
                ds2);

        verifyBinary(rawContentToString(pid,
                PersistencePaths.nonRdfResource(rootResourceId, resourceId(pid, dsId3)).getContentFilePath()),
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
                               final String pid,
                               final String dsId,
                               final DatastreamVersion datastreamVersion) {
        verifyHeaders(session, pid, dsId, datastreamVersion, null);
    }

    private void verifyHeaders(final OcflObjectSession session,
                               final String pid,
                               final String dsId,
                               final DatastreamVersion datastreamVersion,
                               final String versionNumber) {
        final var resourceId = resourceId(pid, dsId);
        try (final var content = session.readContent(resourceId, versionNumber)) {
            final var headers = content.getHeaders();
            assertEquals(resourceId, headers.getId());
            assertEquals(resourceId(pid), headers.getParent());
            assertEquals(InteractionModel.NON_RDF.getUri(), headers.getInteractionModel());
            assertFalse("not AG", headers.isArchivalGroup());
            assertFalse("not root", headers.isObjectRoot());
            assertFalse("not deleted", headers.isDeleted());
            assertEquals(USER, headers.getCreatedBy());
            assertEquals(USER, headers.getLastModifiedBy());
            assertThat(headers.getLastModifiedDate().toString(), containsString(date));
            assertThat(headers.getCreatedDate().toString(), containsString(date));
            if (INLINE.equals(datastreamVersion.getDatastreamInfo().getControlGroup())) {
                assertNotEquals(Long.valueOf(datastreamVersion.getSize()), headers.getContentSize());
            } else {
                assertEquals(Long.valueOf(datastreamVersion.getSize()), headers.getContentSize());
            }
            assertEquals(datastreamVersion.getMimeType(), headers.getMimeType());
            assertEquals(dsId, headers.getFilename());
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
                                   final String pid,
                                   final String dsId) {
        verifyDescHeaders(session, pid, dsId, null);
    }

    private void verifyDescHeaders(final OcflObjectSession session,
                                   final String pid,
                                   final String dsId,
                                   final String versionNumber) {
        try (final var content = session.readContent(medadataId(pid, dsId), versionNumber)) {
            final var headers = content.getHeaders();
            assertEquals(medadataId(pid, dsId), headers.getId());
            assertEquals(resourceId(pid, dsId), headers.getParent());
            assertEquals(InteractionModel.NON_RDF_DESCRIPTION.getUri(), headers.getInteractionModel());
            assertFalse("not AG", headers.isArchivalGroup());
            assertFalse("not root", headers.isObjectRoot());
            assertFalse("not deleted", headers.isDeleted());
            assertEquals(USER, headers.getCreatedBy());
            assertEquals(USER, headers.getLastModifiedBy());
            assertThat(headers.getLastModifiedDate().toString(), containsString(date));
            assertThat(headers.getCreatedDate().toString(), containsString(date));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void verifyObjectHeaders(final OcflObjectSession session,
                                     final String pid) {
        verifyObjectHeaders(session, pid, null);
    }

    private void verifyObjectHeaders(final OcflObjectSession session,
                                     final String pid,
                                     final String versionNumber) {
        try (final var content = session.readContent(resourceId(pid), versionNumber)) {
            final var headers = content.getHeaders();
            assertEquals(resourceId(pid), headers.getId());
            assertEquals(FCREPO_ROOT, headers.getParent());
            assertEquals(InteractionModel.BASIC_CONTAINER.getUri(), headers.getInteractionModel());
            assertTrue("is AG", headers.isArchivalGroup());
            assertTrue("is root", headers.isObjectRoot());
            assertFalse("not deleted", headers.isDeleted());
            assertEquals(USER, headers.getCreatedBy());
            assertEquals(USER, headers.getLastModifiedBy());
            assertThat(headers.getLastModifiedDate().toString(), containsString(date));
            assertThat(headers.getCreatedDate().toString(), containsString(date));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void verifyDescRdf(final OcflObjectSession session,
                               final String pid,
                               final String dsId,
                               final DatastreamVersion datastreamVersion) {
        verifyDescRdf(session, pid, dsId, datastreamVersion, null);
    }

    private void verifyDescRdf(final OcflObjectSession session,
                               final String pid,
                               final String dsId,
                               final DatastreamVersion datastreamVersion,
                               final String versionNumber) {
        try (final var content = session.readContent(medadataId(pid, dsId), versionNumber)) {
            final var value = IOUtils.toString(content.getContentStream().get());
            assertThat(value, allOf(
                    containsString(datastreamVersion.getLabel()),
                    containsString(datastreamVersion.getFormatUri()),
                    containsString("objState")
            ));
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

    private String contentToString(final OcflObjectSession session, final String pid) {
        return contentVersionToString(session, pid, null);
    }

    private String contentVersionToString(final OcflObjectSession session,
                                          final String pid,
                                          final String versionNumber) {
        try (final var content = session.readContent(resourceId(pid), versionNumber)) {
            return IOUtils.toString(content.getContentStream().get());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String contentToString(final OcflObjectSession session, final String pid, final String dsId) {
        return contentVersionToString(session, pid, dsId, null);
    }

    private String contentVersionToString(final OcflObjectSession session,
                                          final String pid,
                                          final String dsId,
                                          final String versionNumber) {
        try (final var content = session.readContent(resourceId(pid, dsId), versionNumber)) {
            return IOUtils.toString(content.getContentStream().get());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String rawContentToString(final String pid, final String path) {
        try (final var content = ocflRepo.getObject(ObjectVersionId.head(resourceId(pid)))
                .getFile(path).getStream()) {
            return IOUtils.toString(content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String rawContentVersionToString(final String pid, final String path, final String versionNumber) {
        try (final var content = ocflRepo.getObject(ObjectVersionId.version(resourceId(pid), versionNumber))
                .getFile(path).getStream()) {
            return IOUtils.toString(content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void rawVerifyDoesNotExist(final String pid, final String path) {
        assertFalse(String.format("object %s not contain path %s", pid, path),
                ocflRepo.describeVersion(ObjectVersionId.head(resourceId(pid))).containsFile(path));
    }

    private ArchiveGroupHandler createHandler(final MigrationType migrationType,
                                              final boolean addExtensions,
                                              final boolean deleteInactive) {
        if (migrationType == MigrationType.PLAIN_OCFL) {
            return new ArchiveGroupHandler(plainSessionFactory, migrationType, addExtensions, deleteInactive, USER);
        } else {
            return new ArchiveGroupHandler(sessionFactory, migrationType, addExtensions, deleteInactive, USER);
        }
    }

    private ObjectVersionReference objectVersionReference(final String pid,
                                                          final boolean isFirst,
                                                          final List<DatastreamVersion> datastreamVersions) {
        return objectVersionReference(pid, isFirst, OBJ_ACTIVE, datastreamVersions);
    }

    private ObjectVersionReference objectVersionReference(final String pid,
                                                          final boolean isFirst,
                                                          final String state,
                                                          final List<DatastreamVersion> datastreamVersions) {
        final var mock = Mockito.mock(ObjectVersionReference.class);
        when(mock.getObjectInfo()).thenReturn(new DefaultObjectInfo(pid, pid));
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
        return datastreamVersion(datastreamId, isFirst, controlGroup, mimeType, content, DS_ACTIVE, externalUrl);
    }

    private DatastreamVersion datastreamVersion(final String datastreamId,
                                                final boolean isFirst,
                                                final String controlGroup,
                                                final String mimeType,
                                                final String content,
                                                final String state,
                                                final String externalUrl) {
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
        when(mock.getLabel()).thenReturn(datastreamId + "-label");
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

    private String resourceId(final String pid) {
        return FCREPO_ROOT + pid;
    }

    private String resourceId(final String pid, final String dsId) {
        return resourceId(pid) + "/" + dsId;
    }

    private String medadataId(final String pid, final String dsId) {
        return resourceId(pid, dsId) + "/fcr:metadata";
    }

    private void verifyFcrepoNotExists(final String pid) {
        final var count = ocflRepo.describeVersion(ObjectVersionId.head(resourceId(pid))).getFiles().stream()
                .map(FileDetails::getPath)
                .filter(file -> file.startsWith(".fcrepo/"))
                .count();
        assertEquals(0, count);
    }

    private void verifyContentNotExists(final OcflObjectSession session, final String resourceId) {
        try (final var content = session.readContent(resourceId)) {
            assertTrue("Content should not exist", content.getContentStream().isEmpty());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void verifyResourceDeleted(final OcflObjectSession session, final String resourceId) {
        final var headers = session.readHeaders(resourceId);
        assertTrue("resource " + resourceId + " should be deleted", headers.isDeleted());
        verifyContentNotExists(session, resourceId);
    }

}
