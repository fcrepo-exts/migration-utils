package org.fcrepo.migration;

import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleIdEncapsulationLayoutConfig;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.Assert.assertTrue;

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
        } catch (IOException io) {
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
}