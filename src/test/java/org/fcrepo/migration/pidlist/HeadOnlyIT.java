package org.fcrepo.migration.pidlist;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.xml.stream.XMLStreamException;

import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.FileDetails;
import edu.wisc.library.ocfl.api.model.ObjectDetails;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import edu.wisc.library.ocfl.core.path.mapper.LogicalPathMappers;
import edu.wisc.library.ocfl.core.storage.OcflStorageBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.fcrepo.migration.MigrationType;
import org.fcrepo.migration.Migrator;
import org.fcrepo.migration.OcflSessionFactoryFactoryBean;
import org.fcrepo.migration.ResourceMigrationType;
import org.fcrepo.migration.foxml.LegacyFSIDResolver;
import org.fcrepo.migration.foxml.NativeFoxmlDirectoryObjectSource;
import org.fcrepo.migration.handlers.ObjectAbstractionStreamingFedoraObjectHandler;
import org.fcrepo.migration.handlers.ocfl.ArchiveGroupHandler;
import org.fcrepo.storage.ocfl.OcflObjectSessionFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests to test migration of head only datastream cases
 *
 * @author mikejritter
 */
public class HeadOnlyIT {

    private final String user = "fedoraAdmin";
    private final String idPrefix = "info:fedora/";
    private final String testPid = idPrefix + "example:1";
    private final boolean disableChecksum = false;
    private final boolean disableDc = false;

    private final DigestAlgorithm digestAlgorithm = DigestAlgorithm.sha512;
    private final MigrationType migrationType = MigrationType.FEDORA_OCFL;

    private LegacyFSIDResolver idResolver;
    private NativeFoxmlDirectoryObjectSource objectSource;
    private OcflObjectSessionFactory ocflObjectSessionFactory;

    private File storage;
    private File staging;
    private File workingDir;

    @Before
    public void setup() throws Exception {
        // Create directories expected in this test
        storage = new File("target/test/ocfl/head-it/storage");
        staging = new File("target/test/ocfl/head-it/staging");
        workingDir = new File("target/test/ocfl/head-it/work");

        if (storage.exists()) {
            FileUtils.forceDelete(storage);
        }
        if (staging.exists()) {
            FileUtils.forceDelete(staging);
        }
        if (workingDir.exists()) {
            FileUtils.forceDelete(workingDir);
        }

        storage.mkdirs();
        staging.mkdirs();
        workingDir.mkdirs();

        // Init Fedora3 classes
        final var f3Hostname = "fedora.info";

        final var f3ObjectDir = new File("src/test/resources/legacyFS-multiple-versions/objects/2015/0430/16/01");
        final var f3DatastreamDir =
            new File("src/test/resources/legacyFS-multiple-versions/datastreams/2015/0430/16/01");

        idResolver = new LegacyFSIDResolver(f3DatastreamDir);
        objectSource = new NativeFoxmlDirectoryObjectSource(f3ObjectDir, idResolver, f3Hostname);

        // Init OCFL classes
        final var userUri = idPrefix + user;
        ocflObjectSessionFactory =
            new OcflSessionFactoryFactoryBean(storage.toPath(), staging.toPath(), migrationType, user, userUri,
                                              digestAlgorithm, disableChecksum).getObject();
    }

    @Test
    public void testMigrateHeadOnly() throws XMLStreamException {
        final var agh = archiveGroupHandler(true);
        final var handler = new ObjectAbstractionStreamingFedoraObjectHandler(agh);
        final var migrator = new Migrator();
        migrator.setSource(objectSource);
        migrator.setHandler(handler);

        migrator.run();

        // check that there's only single version for each datastream
        final var ocflRepository = repository();
        final var objectDetails = ocflRepository.describeObject(testPid);
        final var fileVersions = collectDatastreamVersions(objectDetails);

        fileVersions.values().forEach(versions -> assertEquals(1, versions.size()));
    }

    @Test
    public void testMigrateHeadOnlyDisabled() throws XMLStreamException {
        final var agh = archiveGroupHandler(false);
        final var handler = new ObjectAbstractionStreamingFedoraObjectHandler(agh);
        final var migrator = new Migrator();
        migrator.setSource(objectSource);
        migrator.setHandler(handler);

        migrator.run();
        final var ocflRepository = repository();
        final var objectDetails = ocflRepository.describeObject(testPid);
        final var fileVersions = collectDatastreamVersions(objectDetails);

        // from the foxml, two datastreams have versions: DS1 and DC
        final var dcVersions = fileVersions.get("/DC");
        final var ds1Versions = fileVersions.get("/DS1");

        assertEquals(2, dcVersions.size());
        assertEquals(2, ds1Versions.size());
    }

    private ArchiveGroupHandler archiveGroupHandler(final boolean headOnly) {
        final boolean foxmlFile = false;
        final boolean deleteInactive = false;
        final boolean datastreamExtensions = false;
        final ResourceMigrationType resourceMigrationType = ResourceMigrationType.ARCHIVAL;
        return new ArchiveGroupHandler(ocflObjectSessionFactory, migrationType, resourceMigrationType,
                                       datastreamExtensions, deleteInactive, foxmlFile, user, idPrefix,
                                       headOnly, disableChecksum, disableDc);
    }

    private OcflRepository repository() {
        final var ocflStorage = OcflStorageBuilder.builder()
            .fileSystem(storage.toPath())
            .build();

        final var logicalPathMapper = SystemUtils.IS_OS_WINDOWS ? LogicalPathMappers.percentEncodingWindowsMapper()
                                                                : LogicalPathMappers.percentEncodingLinuxMapper();

        return new OcflRepositoryBuilder().storage(ocflStorage)
                                          .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                                          .logicalPathMapper(logicalPathMapper)
                                          .workDir(staging.toPath())
                                          .build();
    }

    private Map<String, Set<String>> collectDatastreamVersions(final ObjectDetails objectDetails) {
        return objectDetails.getVersionMap()
                            .values().stream()
                            .flatMap(details -> details.getFiles().stream())
                            .map(FileDetails::getStorageRelativePath)
                            .filter(string -> !string.contains(".fcrepo") && !string.contains(".nt"))
                            .collect(Collectors.groupingBy(s -> s.substring(s.lastIndexOf("/")), Collectors.toSet()));
    }
}
