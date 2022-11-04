package org.fcrepo.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.FileDetails;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionDetails;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import edu.wisc.library.ocfl.core.storage.OcflStorageBuilder;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleIdEncapsulationLayoutConfig;
import picocli.CommandLine;

/**
 * @author bcail
 */
public class PicocliIT {

    private Path tmpDir;
    private Path targetDir;
    private Path workingDir;

    @Before
    public void setup() throws IOException {
        tmpDir = Files.createTempDirectory("migration-utils");
        targetDir = tmpDir.resolve("target");
        workingDir = tmpDir.resolve("working");
    }

    @After
    public void tearDown() throws IOException {
        try {
            FileUtils.forceDelete(tmpDir.toFile());
        } catch (final IOException io) {
            System.err.println("Error cleaning up " + tmpDir.toString());
            io.printStackTrace();
        }
    }

    private boolean checkDirForNamaste(final Path targetDir) throws IOException {
        return Files.list(targetDir).map(Path::getFileName).map(Path::toString)
                .anyMatch(e -> e.startsWith("0=ocfl_1."));
    }

    @Test
    public void testPlainOcfl() throws Exception {
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "PLAIN_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);

        cmd.execute(args);
        assertTrue(checkDirForNamaste(targetDir));
        final Path baseDir = targetDir.resolve("5b5").resolve("62d").resolve("d69")
                .resolve("5b562dd698f17e3198e007e6f77f9e48f20a556c6bae84e6fc8d98544831daa6");
        final File inventory = baseDir.resolve("inventory.json").toFile();
        assertTrue(inventory.exists());
        assertTrue(Files.list(workingDir).anyMatch(element -> element.endsWith("index")));
        assertTrue(Files.list(workingDir).anyMatch(element -> element.endsWith("pid")));
    }

    @Test
    public void testPlainOcflEmptyIdPrefix() throws Exception {
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "PLAIN_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01",
                "--id-prefix", ""};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);

        final int result = cmd.execute(args);
        assertEquals(0, result);
        assertTrue(checkDirForNamaste(targetDir));
        final Path baseDir = targetDir.resolve("750").resolve("677").resolve("e9b")
                .resolve("750677e9b953845ba5069d27a3775fbced186987fd0f4a8c968ac457a7d415a8");
        final File inventory = baseDir.resolve("inventory.json").toFile();
        assertTrue(inventory.exists());
        assertTrue(Files.list(workingDir).anyMatch(element -> element.endsWith("index")));
        assertTrue(Files.list(workingDir).anyMatch(element -> element.endsWith("pid")));
    }

    @Test
    public void testFedoraOcflCantChangeIdPrefix() throws Exception {
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "FEDORA_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01",
                "--id-prefix", ""};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);

        final int result = cmd.execute(args);
        assertEquals(1, result);
    }

    @Test
    public void testPlainOcflNoWorkingDirOption() throws Exception {
        final String[] args = {"--target-dir", targetDir.toString(),
                "--source-type", "LEGACY","--migration-type", "PLAIN_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);

        cmd.execute(args);
        final Path workingDir = Path.of(System.getProperty("user.dir"));
        assertTrue(checkDirForNamaste(targetDir));
        assertTrue(Files.list(workingDir).anyMatch(element -> element.endsWith("index")));
        assertTrue(Files.list(workingDir).anyMatch(element -> element.endsWith("pid")));
    }

    @Test
    public void testFedoraOcfl() throws Exception {
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "FEDORA_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);

        cmd.execute(args);
        assertTrue(checkDirForNamaste(targetDir.resolve("data").resolve("ocfl-root")));
        assertTrue(Files.list(workingDir).anyMatch(element -> element.endsWith("index")));
        assertTrue(Files.list(workingDir).anyMatch(element -> element.endsWith("pid")));
    }

    @Test
    public void testExistingRepoDifferentStorageLayout() throws Exception {
        //create repo with different storage layout
        final var ocflRepo =  new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleIdEncapsulationLayoutConfig())
                .storage(OcflStorageBuilder.builder().fileSystem(targetDir).build())
                .workDir(tmpDir)
                .build();
        assertTrue(checkDirForNamaste(targetDir));

        //migrate object into it
        final Path workingDir = tmpDir.resolve("working");
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "PLAIN_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01",
                "--id-prefix", ""};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);
        cmd.execute(args);

        //verify that the correct storage layout was used - encapsulation directory is the encoded object id
        assertTrue(Files.list(targetDir.resolve("750").resolve("677").resolve("e9b"))
                .anyMatch(f -> f.endsWith("example%3a1")));
    }

    @Test
    public void testMigrateFoxmlFileInsteadOfPropertyFiles() throws Exception {
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY", "--migration-type", "PLAIN_OCFL",
                "--datastreams-dir", "src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01",
                "--id-prefix", "", "--foxml-file"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);
        cmd.execute(args);

        final Path baseDir = targetDir.resolve("750").resolve("677").resolve("e9b")
                .resolve("750677e9b953845ba5069d27a3775fbced186987fd0f4a8c968ac457a7d415a8");
        final File inventory = baseDir.resolve("inventory.json").toFile();
        assertTrue(inventory.exists());
        final var ocflRepo =  new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .storage(OcflStorageBuilder.builder().fileSystem(targetDir).build())
                .workDir(workingDir)
                .build();
        final var object = ocflRepo.getObject(ObjectVersionId.head("example:1"));
        final ArrayList<String> files = new ArrayList<String>();
        for (final var file: object.getFiles()) {
            files.add(file.getPath());
        }
        final var expectedFiles = new ArrayList<String>();
        expectedFiles.add("AUDIT");
        expectedFiles.add("DS2");
        expectedFiles.add("DS1");
        expectedFiles.add("DS4");
        expectedFiles.add("DS3");
        expectedFiles.add("DC");
        assertEquals(expectedFiles, files);
        //now check for a FOXML, which should show up in a previous version
        final var versions = ocflRepo.describeObject("example:1").getVersionMap().values();
        boolean foundFoxml = false;
        for (final VersionDetails v : versions) {
            for (final FileDetails f : v.getFiles()) {
                if (f.getPath().equals("FOXML")) {
                    foundFoxml = true;
                    break;
                }
            }
        }
        assertTrue(foundFoxml);
    }

    @Test
    public void testInvalidDigestAlgorithm() throws Exception {
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "PLAIN_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01",
                "--algorithm", "sha384"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);

        final int result = cmd.execute(args);
        assertEquals(1, result);
    }

    /**
     * MD5 is a supported algorithm under an OCFL extension, but we don't support it.
     */
    @Test
    public void testInvalidForUsDigestAlgorithm() {
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "PLAIN_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01",
                "--algorithm", "md5"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);

        final int result = cmd.execute(args);
        assertEquals(1, result);
    }

    @Test
    public void testSha256DigestAlgorithm() throws Exception {
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "PLAIN_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01",
                "--algorithm", "sha256"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);

        final int result = cmd.execute(args);
        assertEquals(0, result);
        assertTrue(checkDirForNamaste(targetDir));
        final Path baseDir = targetDir.resolve("5b5").resolve("62d").resolve("d69")
                .resolve("5b562dd698f17e3198e007e6f77f9e48f20a556c6bae84e6fc8d98544831daa6");
        final File inventory = baseDir.resolve("inventory.json").toFile();
        assertTrue(inventory.exists());
        validateManifests(inventory, "SHA-256", baseDir);
    }

    @Test
    public void testPlainOcflObjectAlreadyExistsInOcfl() throws Exception {
        final var pid = "example:1";
        final var ocflRepo =  new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleIdEncapsulationLayoutConfig())
                .storage(OcflStorageBuilder.builder().fileSystem(targetDir).build())
                .workDir(tmpDir)
                .build();
        ocflRepo.updateObject(ObjectVersionId.head(pid), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),"file1");
        });
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "PLAIN_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01",
                "--id-prefix", ""};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);
        final int result = cmd.execute(args);
        assertEquals(1, result); //should fail because object already exists
    }

    @Test
    public void testFedoraOcflObjectAlreadyExistsInOcfl() throws Exception {
        final var ocflObjectId = "info:fedora/example:1";
        final var ocflRepo =  new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleIdEncapsulationLayoutConfig())
                .storage(OcflStorageBuilder.builder().fileSystem(targetDir).build())
                .workDir(tmpDir)
                .build();
        ocflRepo.updateObject(ObjectVersionId.head(ocflObjectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),"file1");
        });
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "PLAIN_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);
        final int result = cmd.execute(args);
        assertEquals(1, result); //should fail because object already exists
    }

    @Test
    public void testInvalidChecksumErrorsPlain() throws Exception {
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "PLAIN_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS-invalid-checksum/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS-invalid-checksum/objects/2015/0430/16/01"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);
        final int result = cmd.execute(args);
        assertEquals(1, result); //should fail because of invalid checksum
    }

    @Test
    public void testInvalidChecksumErrorsFedora() throws Exception {
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "FEDORA_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS-invalid-checksum/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS-invalid-checksum/objects/2015/0430/16/01"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);
        final int result = cmd.execute(args);
        assertEquals(1, result); //should fail because of invalid checksum
    }

    @Test
    public void testInvalidChecksumCanBeAllowedPlain() throws Exception {
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "PLAIN_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS-invalid-checksum/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS-invalid-checksum/objects/2015/0430/16/01",
                "--no-checksum-validation"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);
        final int result = cmd.execute(args);
        assertEquals(0, result); //should succeed because checksum validation is disabled
    }

    @Test
    public void testInvalidChecksumCanBeAllowedFedora() throws Exception {
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "FEDORA_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS-invalid-checksum/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS-invalid-checksum/objects/2015/0430/16/01",
                "--no-checksum-validation"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);
        final int result = cmd.execute(args);
        assertEquals(0, result); //should succeed because checksum validation is disabled
    }

    @Test
    public void handleOutOfOrderDatastreamVersions() throws Exception {
        final var ocflObjectId = "info:fedora/example:1";
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "FEDORA_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS-out-of-order/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS-out-of-order/objects/2015/0430/16/01"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);

        final int result = cmd.execute(args);
        assertEquals(0, result);

        final var ocflRepo = createOcflRepo();

        final var obj = ocflRepo.getObject(ObjectVersionId.head(ocflObjectId));
        try (final var stream = obj.getFile("DS1").getStream()) {
            assertEquals("\n<test>\n  This is a test that was edited.\n</test>\n",
                    IOUtils.toString(stream, StandardCharsets.UTF_8));
        }
    }

    /**
     * Validate the manifest digests in an inventory file.
     * @param inventory the inventory file
     * @param digestAlgo the digest algorithm
     * @param baseDir the path of the OCFL object
     * @throws IOException issues opening the inventory file.
     * @throws NoSuchAlgorithmException issues creating a MessageDigest.
     */
    private void validateManifests(final File inventory, final String digestAlgo, final Path baseDir)
            throws IOException, NoSuchAlgorithmException {
        final var manifests = getManifests(inventory);
        final MessageDigest md = MessageDigest.getInstance(digestAlgo);
        for (final var entry : manifests.entrySet()) {
            final File f = baseDir.resolve(entry.getKey()).toFile();
            assertTrue(f.exists());
            final String digest = new String(Hex.encodeHex(DigestUtils.digest(md, new FileInputStream(f))));
            assertEquals(entry.getValue(), digest);
        }
    }

    /**
     * Parse the manifest section out of an OCFL inventory file and return a map of filename -> hash
     * @param inventory the OCFL inventory file
     * @return map of file paths from the OCFL object root and their digests
     * @throws IOException issues opening the inventory file.
     */
    private Map<String, String> getManifests(final File inventory) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final JsonNode rootNode = mapper.readTree(inventory);
        final JsonNode manifestNode = rootNode.findValues("manifest").get(0);
        final Map<String, String> fileManifest = new HashMap<>();
        final var fieldIter = manifestNode.fields();
        while (fieldIter.hasNext()) {
            final var entry = fieldIter.next();
            final String hash = entry.getKey();
            if (entry.getValue().isArray()) {
                // More than one file with the same hash
                entry.getValue().spliterator().forEachRemaining(file -> fileManifest.put(file.asText(), hash));
            } else {
                fileManifest.put(entry.getValue().asText(), hash);
            }
        }
        return fileManifest;
    }

    private OcflRepository createOcflRepo() {
        return new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .storage(OcflStorageBuilder.builder().fileSystem(targetDir.resolve("data/ocfl-root")).build())
                .workDir(workingDir)
                .build();
    }

}
