/*
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

import static edu.wisc.library.ocfl.api.util.Enforce.expressionTrue;
import static edu.wisc.library.ocfl.api.util.Enforce.notNull;
import static org.slf4j.LoggerFactory.getLogger;
import static picocli.CommandLine.Help.Visibility.ALWAYS;

import java.io.File;
import java.util.concurrent.Callable;

import org.apache.jena.sys.JenaSystem;
import org.fcrepo.migration.foxml.AkubraFSIDResolver;
import org.fcrepo.migration.foxml.ArchiveExportedFoxmlDirectoryObjectSource;
import org.fcrepo.migration.foxml.InternalIDResolver;
import org.fcrepo.migration.foxml.LegacyFSIDResolver;
import org.fcrepo.migration.foxml.NativeFoxmlDirectoryObjectSource;
import org.fcrepo.migration.handlers.ObjectAbstractionStreamingFedoraObjectHandler;
import org.fcrepo.migration.handlers.ocfl.ArchiveGroupHandler;
import org.fcrepo.migration.metrics.PrometheusActuator;
import org.fcrepo.migration.pidlist.ResumePidListManager;
import org.fcrepo.migration.pidlist.UserProvidedPidListManager;
import org.fcrepo.storage.ocfl.OcflObjectSessionFactory;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import edu.wisc.library.ocfl.api.DigestAlgorithmRegistry;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;


/**
 * This class provides a simple CLI for running and configuring migration-utils
 * - See README.md for usage details
 *
 * @author Remi Malessa
 * @author awoods
 * @since 2019-11-15
 */
@Command(name = "migration-utils", mixinStandardHelpOptions = true, sortOptions = false,
        version = "Migration Utils - 4.4.1.b")
public class PicocliMigrator implements Callable<Integer> {

    private static final Logger LOGGER = getLogger(PicocliMigrator.class);

    private enum F3SourceTypes {
        AKUBRA, LEGACY, EXPORTED;

        static F3SourceTypes toType(final String v) {
            return valueOf(v.toUpperCase());
        }
    }

    private final String DEFAULT_PREFIX = "info:fedora/";

    @Option(names = {"--source-type", "-t"}, required = true, order = 1,
            description = "Fedora 3 source type. Choices: akubra | legacy | exported")
    private F3SourceTypes f3SourceType;

    @Option(names = {"--datastreams-dir", "-d"}, order = 2,
            description = "Directory containing Fedora 3 datastreams (used with --source-type 'akubra' or 'legacy')")
    private File f3DatastreamsDir;

    @Option(names = {"--objects-dir", "-o"}, order = 3,
            description = "Directory containing Fedora 3 objects (used with --source-type 'akubra' or 'legacy')")
    private File f3ObjectsDir;

    @Option(names = {"--exported-dir", "-e"}, order = 4,
            description = "Directory containing Fedora 3 export (used with --source-type 'exported')")
    private File f3ExportedDir;

    @Option(names = {"--target-dir", "-a"}, required = true, order = 5,
            description = "OCFL storage root directory (data/ocfl-root is created for migration-type FEDORA_OCFL)")
    private File targetDir;

    @Option(names = {"--working-dir", "-i"}, order = 6,
            description = "Directory where supporting state will be written (cached index of datastreams, ...)")
    private File workingDir;

    @Option(names = {"--delete-inactive", "-I"}, defaultValue = "false", showDefaultValue = ALWAYS, order = 18,
            description = "Migrate objects and datastreams in the Inactive state as deleted. Default: false.")
    private boolean deleteInactive;

    @Option(names = {"--migration-type", "-m"}, defaultValue = "FEDORA_OCFL", showDefaultValue = ALWAYS, order = 19,
            description = "Type of OCFL objects to migrate to. Choices: FEDORA_OCFL | PLAIN_OCFL")
    private MigrationType migrationType;

    @Option(names = {"--id-prefix"}, defaultValue = DEFAULT_PREFIX, showDefaultValue = ALWAYS, order = 20,
            description = "Only use this for PLAIN_OCFL migrations: Prefix to add to PIDs for OCFL object IDs"
                + " - defaults to info:fedora/, like Fedora3")
    private String idPrefix;

    @Option(names = {"--foxml-file"}, defaultValue = "false", order = 21,
            description = "Migrate FOXML file as a whole file, instead of creating property files. FOXML file will"
                + " be migrated, then marked as deleted so it doesn't show up as an active file.")
    private boolean foxmlFile;

    @Option(names = {"--limit", "-l"}, defaultValue = "-1", order = 22,
            description = "Limit number of objects to be processed.\n  Default: no limit")
    private int objectLimit;

    @Option(names = {"--resume", "-r"}, defaultValue = "false", showDefaultValue = ALWAYS, order = 23,
            description = "Resume from last successfully migrated Fedora 3 object")
    private boolean resume;

    @Option(names = {"--continue-on-error", "-c"}, defaultValue = "false", showDefaultValue = ALWAYS, order = 24,
            description = "Continue to next PID if an error occurs (instead of exiting). Disabled by default.")
    private boolean continueOnError;

    @Option(names = {"--pid-file", "-p"}, order = 25,
            description = "PID file listing which Fedora 3 objects to migrate")
    private File pidFile;

    @Option(names = {"--extensions", "-x"}, defaultValue = "false", showDefaultValue = ALWAYS, order = 26,
            description = "Add file extensions to migrated datastreams based on mimetype recorded in FOXML")
    private boolean addExtensions;

    @Option(names = {"--f3hostname", "-f"}, defaultValue = "fedora.info", showDefaultValue = ALWAYS, order = 27,
            description = "Hostname of Fedora 3, used for replacing placeholder in 'E' and 'R' datastream URLs")
    private String f3hostname;

    @Option(names = {"--username", "-u"}, defaultValue = "fedoraAdmin", showDefaultValue = ALWAYS, order = 28,
            description = "The username to associate with all of the migrated resources.")
    private String user;

    @Option(names = {"--user-uri", "-U"}, defaultValue = "info:fedora/fedoraAdmin", showDefaultValue = ALWAYS,
            order = 29, description = "The username to associate with all of the migrated resources.")
    private String userUri;

    @Option(names = {"--algorithm"}, defaultValue = "sha512", showDefaultValue = ALWAYS, order = 30,
            description = "The digest algorithm to use in the OCFL objects created. Either sha256 or sha512")
    private String digestAlgorithm;

    @Option(names = {"--no-checksum-validation"}, defaultValue = "false", showDefaultValue = ALWAYS, order = 31,
            description = "Disable validation that datastream content matches Fedora 3 checksum.")
    private boolean disableChecksumValidation;

    @Option(names = {"--enable-metrics"}, defaultValue = "false", showDefaultValue = ALWAYS, order = 32,
            description = "Enable gathering of metrics for a Prometheus instance. " +
                          "\nNote: this requires port 8080 to be free in order for Prometheus to scrape metrics.")
    private boolean enableMetrics;

    @Option(names = {"--debug"}, order = 32, description = "Enables debug logging")
    private boolean debug;

    private File indexDir;

    private File ocflStorageDir;

    /**
     * @param args Command line arguments
     */
    public static void main(final String[] args) {
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);
        cmd.registerConverter(F3SourceTypes.class, F3SourceTypes::toType);
        cmd.setExecutionExceptionHandler(new PicoliMigrationExceptionHandler(migrator));

        cmd.execute(args);
    }

    private static class PicoliMigrationExceptionHandler implements CommandLine.IExecutionExceptionHandler {

        private final PicocliMigrator migrator;

        PicoliMigrationExceptionHandler(final PicocliMigrator migrator) {
            this.migrator = migrator;
        }

        @Override
        public int handleExecutionException(
                final Exception ex,
                final CommandLine commandLine,
                final CommandLine.ParseResult parseResult) {
            commandLine.getErr().println(ex.getMessage());
            if (migrator.debug) {
                ex.printStackTrace(commandLine.getErr());
            }
            commandLine.usage(commandLine.getErr());
            return commandLine.getCommandSpec().exitCodeOnExecutionException();
        }
    }

    private static void setDebugLogLevel() {
        final LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        final ch.qos.logback.classic.Logger logger = loggerContext.getLogger("org.fcrepo.migration");
        logger.setLevel(Level.toLevel("DEBUG"));
    }

    @Override
    public Integer call() throws Exception {

        // Set debug log level if requested
        if (debug) {
            setDebugLogLevel();
        }

        if (migrationType == MigrationType.FEDORA_OCFL && !idPrefix.equals(DEFAULT_PREFIX)) {
            throw new IllegalArgumentException("Can't change the ID Prefix for FEDORA_OCFL migrations");
        }

        if (!digestAlgorithm.equals("sha512") && !digestAlgorithm.equalsIgnoreCase("sha256")) {
            throw new IllegalArgumentException("Invalid algorithm specified, must be one of sha512 or sha256");
        }
        final DigestAlgorithm algorithm = DigestAlgorithmRegistry.getAlgorithm(digestAlgorithm);
        notNull(algorithm, "Invalid algorithm specified, must be one of sha512 or sha256");

        // Pre-processing directory verification
        notNull(targetDir, "targetDir must be provided!");
        if (!targetDir.exists()) {
            targetDir.mkdirs();
        }

        if (workingDir == null) {
            LOGGER.info("No working-dir option passed in - using current directory.");
            workingDir = new File(System.getProperty("user.dir"));
        }
        if (!workingDir.exists()) {
            workingDir.mkdirs();
        }
        indexDir = new File(workingDir, "index");

        if (migrationType == MigrationType.FEDORA_OCFL) {
            // Fedora 6.0.0 expects a data/ocfl-root structure
            ocflStorageDir = targetDir.toPath().resolve("data").resolve("ocfl-root").toFile();
            if (!ocflStorageDir.exists()) {
                ocflStorageDir.mkdirs();
            }
        } else {
            ocflStorageDir = targetDir;
        }

        // Create Staging dir
        final File ocflStagingDir = new File(workingDir, "staging");
        if (!ocflStagingDir.exists()) {
            ocflStagingDir.mkdirs();
        }

        // Create PID list dir
        final File pidDir = new File(workingDir, "pid");
        if (!pidDir.exists()) {
            pidDir.mkdirs();
        }

        // Which F3 source are we using? - verify associated options
        final ObjectSource objectSource;
        InternalIDResolver idResolver = null;
        switch (f3SourceType) {
            case EXPORTED:
                notNull(f3ExportedDir, "f3ExportDir must be used with 'exported' source!");

                objectSource = new ArchiveExportedFoxmlDirectoryObjectSource(f3ExportedDir, f3hostname);
                break;
            case AKUBRA:
                notNull(f3DatastreamsDir, "f3DatastreamsDir must be used with 'akubra' or 'legacy' source!");
                notNull(f3ObjectsDir, "f3ObjectsDir must be used with 'akubra' or 'legacy' source!");
                expressionTrue(f3ObjectsDir.exists(), f3ObjectsDir, "f3ObjectsDir must exist! " +
                        f3ObjectsDir.getAbsolutePath());

                idResolver = new AkubraFSIDResolver(indexDir, f3DatastreamsDir);
                objectSource = new NativeFoxmlDirectoryObjectSource(f3ObjectsDir, idResolver, f3hostname);
                break;
            case LEGACY:
                notNull(f3DatastreamsDir, "f3DatastreamsDir must be used with 'akubra' or 'legacy' source!");
                notNull(f3ObjectsDir, "f3ObjectsDir must be used with 'akubra' or 'legacy' source!");
                expressionTrue(f3ObjectsDir.exists(), f3ObjectsDir, "f3ObjectsDir must exist! " +
                        f3ObjectsDir.getAbsolutePath());

                idResolver = new LegacyFSIDResolver(indexDir, f3DatastreamsDir);
                objectSource = new NativeFoxmlDirectoryObjectSource(f3ObjectsDir, idResolver, f3hostname);
                break;
            default:
                throw new RuntimeException("Should never happen");
        }

        // setup HttpServer + micrometer for publishing metrics
        final PrometheusActuator actuator = new PrometheusActuator(enableMetrics);
        actuator.start();

        final OcflObjectSessionFactory ocflSessionFactory = new OcflSessionFactoryFactoryBean(ocflStorageDir.toPath(),
                ocflStagingDir.toPath(), migrationType, user, userUri, algorithm, disableChecksumValidation)
                .getObject();

        final FedoraObjectVersionHandler archiveGroupHandler =
                new ArchiveGroupHandler(ocflSessionFactory, migrationType, addExtensions, deleteInactive, foxmlFile,
                        user, idPrefix, disableChecksumValidation);
        final StreamingFedoraObjectHandler objectHandler = new ObjectAbstractionStreamingFedoraObjectHandler(
                archiveGroupHandler);

        // PID-list-managers
        // - Resume PID manager: the second arg is "acceptAll". If resuming, we do not "acceptAll")
        final ResumePidListManager resumeManager = new ResumePidListManager(pidDir, !resume);

        // - PID-list manager
        final UserProvidedPidListManager pidListManager = new UserProvidedPidListManager(pidFile);

        final Migrator migrator = new Migrator();
        migrator.setLimit(objectLimit);
        migrator.setSource(objectSource);
        migrator.setHandler(objectHandler);
        migrator.setResumePidListManager(resumeManager);
        migrator.setUserProvidedPidListManager(pidListManager);
        migrator.setContinueOnError(continueOnError);

        // init jena because sometimes it doesn't init cleanly by default for whatever reason
        JenaSystem.init();

        try {
            migrator.run();
        } finally {
            ocflSessionFactory.close();
            if (idResolver != null) {
                idResolver.close();
            }
            FileUtils.deleteDirectory(ocflStagingDir);
            actuator.stop();
        }

        return 0;
    }

}
