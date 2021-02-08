package org.fcrepo.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleIdEncapsulationLayoutConfig;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import picocli.CommandLine;

/**
 * @author bcail
 */
public class PicocliIT {

    private Path tmpDir;

    @Before
    public void setup() throws IOException {
        tmpDir = Files.createTempDirectory("migration-utils");
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

    @Test
    public void testPlainOcfl() throws Exception {
        final Path targetDir = tmpDir.resolve("target");
        final Path workingDir = tmpDir.resolve("working");
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "PLAIN_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);

        cmd.execute(args);
        assertTrue(Files.list(targetDir).anyMatch(element -> element.endsWith("0=ocfl_1.0")));
        assertTrue(Files.list(workingDir).anyMatch(element -> element.endsWith("index")));
        assertTrue(Files.list(workingDir).anyMatch(element -> element.endsWith("pid")));
    }

    @Test
    public void testPlainOcflNoWorkingDirOption() throws Exception {
        final Path targetDir = tmpDir.resolve("target");
        final String[] args = {"--target-dir", targetDir.toString(),
                "--source-type", "LEGACY","--migration-type", "PLAIN_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);

        cmd.execute(args);
        final Path workingDir = Path.of(System.getProperty("user.dir"));
        assertTrue(Files.list(targetDir).anyMatch(element -> element.endsWith("0=ocfl_1.0")));
        assertTrue(Files.list(workingDir).anyMatch(element -> element.endsWith("index")));
        assertTrue(Files.list(workingDir).anyMatch(element -> element.endsWith("pid")));
    }

    @Test
    public void testFedoraOcfl() throws Exception {
        final Path targetDir = tmpDir.resolve("target");
        final Path workingDir = tmpDir.resolve("working");
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "FEDORA_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);

        cmd.execute(args);
        assertTrue(Files.list(targetDir.resolve("data").resolve("ocfl-root"))
                .anyMatch(element -> element.endsWith("0=ocfl_1.0")));
        assertTrue(Files.list(workingDir).anyMatch(element -> element.endsWith("index")));
        assertTrue(Files.list(workingDir).anyMatch(element -> element.endsWith("pid")));
    }

    @Test
    public void testExistingRepoDifferentStorageLayout() throws Exception {
        //create repo with different storage layout
        final Path targetDir = tmpDir.resolve("target");
        final var ocflRepo =  new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleIdEncapsulationLayoutConfig())
                .storage(FileSystemOcflStorage.builder().repositoryRoot(targetDir).build())
                .workDir(tmpDir)
                .build();
        assertTrue(Files.list(targetDir).anyMatch(element -> element.endsWith("0=ocfl_1.0")));

        //migrate object into it
        final Path workingDir = tmpDir.resolve("working");
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "PLAIN_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);
        cmd.execute(args);

        //verify that the correct storage layout was used - encapsulation directory is the encoded object id
        assertTrue(Files.list(targetDir.resolve("5b5").resolve("62d").resolve("d69"))
                .anyMatch(f -> f.endsWith("info%3afedora%2fexample%3a1")));
    }

    @Test
    public void testInvalidDigestAlgorithm() throws Exception {
        final Path targetDir = tmpDir.resolve("target");
        final Path workingDir = tmpDir.resolve("working");
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
        final Path targetDir = tmpDir.resolve("target");
        final Path workingDir = tmpDir.resolve("working");
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
        final Path targetDir = tmpDir.resolve("target");
        final Path workingDir = tmpDir.resolve("working");
        final String[] args = {"--target-dir", targetDir.toString(), "--working-dir", workingDir.toString(),
                "--source-type", "LEGACY","--migration-type", "PLAIN_OCFL",
                "--datastreams-dir","src/test/resources/legacyFS/datastreams/2015/0430/16/01",
                "--objects-dir", "src/test/resources/legacyFS/objects/2015/0430/16/01",
                "--algorithm", "sha256"};
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);

        final int result = cmd.execute(args);
        assertEquals(0, result);
        assertTrue(Files.list(targetDir).anyMatch(element -> element.endsWith("0=ocfl_1.0")));
        final Path baseDir = targetDir.resolve("5b5").resolve("62d").resolve("d69")
                .resolve("5b562dd698f17e3198e007e6f77f9e48f20a556c6bae84e6fc8d98544831daa6");
        final File inventory = baseDir.resolve("inventory.json").toFile();
        assertTrue(inventory.exists());
        validateManifests(inventory, "SHA-256", baseDir);
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
}