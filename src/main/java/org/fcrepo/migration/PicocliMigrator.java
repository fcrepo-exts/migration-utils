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

import static org.slf4j.LoggerFactory.getLogger;
import static picocli.CommandLine.Help.Visibility.ALWAYS;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import edu.wisc.library.ocfl.api.util.Enforce;
import org.apache.commons.io.FileUtils;
import org.fcrepo.migration.f4clients.OCFLFedora4Client;
import org.fcrepo.migration.f4clients.OCFLFedora4Client.ObjectIdMapperType;
import org.fcrepo.migration.foxml.AkubraFSIDResolver;
import org.fcrepo.migration.foxml.ArchiveExportedFoxmlDirectoryObjectSource;
import org.fcrepo.migration.foxml.InternalIDResolver;
import org.fcrepo.migration.foxml.LegacyFSIDResolver;
import org.fcrepo.migration.foxml.NativeFoxmlDirectoryObjectSource;
import org.fcrepo.migration.handlers.ObjectAbstractionStreamingFedoraObjectHandler;
import org.fcrepo.migration.handlers.VersionAbstractionFedoraObjectHandler;
import org.fcrepo.migration.handlers.ocfl.ArchiveGroupHandler;
import org.fcrepo.migration.handlers.ocfl.HackyOcflDriver;
import org.fcrepo.migration.handlers.ocfl.OcflDriver;
import org.fcrepo.migration.pidlist.PidListManager;
import org.fcrepo.migration.pidlist.ResumePidListManager;
import org.slf4j.Logger;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;


/**
 * @author Remi Malessa
 * @author awoods
 * @since 2019-11-15
 */
@Command(name = "migration-utils", mixinStandardHelpOptions = true, sortOptions = false,
        version = "Migration Utils - 4.4.1")
public class PicocliMigrator implements Callable<Integer> {

    private static final Logger LOGGER = getLogger(PicocliMigrator.class);

    private enum F3SourceTypes {
        AKUBRA, LEGACY, EXPORTED;

        static F3SourceTypes toType(final String v) {
            return valueOf(v.toUpperCase());
        }
    }

    @Option(names = {"--source-type", "-t"}, required = true, order = 1,
            description = "Fedora 3 source type. Choices: AKUBRA | LEGACY | EXPORTED")
    private F3SourceTypes f3SourceType;

    @Option(names = {"--datastreams-dir", "-d"}, order = 2,
            description = "Directory containing Fedora 3 datastreams (used with --source-type AKUBRA or LEGACY)")
    private File f3DatastreamsDir;

    @Option(names = {"--objects-dir", "-o"}, order = 3,
            description = "Directory containing Fedora 3 objects (used with --source-type AKUBRA or LEGACY)")
    private File f3ObjectsDir;

    @Option(names = {"--exported-dir", "-e"}, order = 4,
            description = "Directory containing Fedora 3 export (used with --source-type EXPORTED)")
    private File f3ExportedDir;

    @Option(names = {"--working-dir", "-w"}, required = true, order = 5,
            description = "Directory where OCFL storage root and supporting state will be written")
    private File workingDir;

    @Option(names = {"--layout", "-y"}, defaultValue = "FLAT", showDefaultValue = ALWAYS, order = 20,
            description = "OCFL layout of storage root")
    private ObjectIdMapperType ocflLayout;

    @Option(names = {"--limit", "-l"}, defaultValue = "-1", showDefaultValue = ALWAYS, order = 21,
            description = "Limit number of objects to be processed.")
    private int objectLimit;

    @Option(names = {"--resume", "-r"}, defaultValue = "false", showDefaultValue = ALWAYS, order = 22,
            description = "Resume from last successfully migrated Fedora 3 object")
    private boolean resume;

    @Option(names = {"--debug"}, order = 30, description = "Enables debug logging")
    private boolean debug;


    /**
     * @param args
     */
    public static void main(final String[] args) {
        final PicocliMigrator migrator = new PicocliMigrator();
        final CommandLine cmd = new CommandLine(migrator);
        cmd.registerConverter(F3SourceTypes.class, F3SourceTypes::toType);
        cmd.setExecutionExceptionHandler(new PicoliMigrationExceptionHandler(migrator));

        cmd.execute(args);
    }

    private static class PicoliMigrationExceptionHandler implements CommandLine.IExecutionExceptionHandler {

        private PicocliMigrator migrator;

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

    @Override
    public Integer call() throws Exception {

        // Pre-processing directory verification
        Enforce.notNull(workingDir, "workingDir must be provided!");
        if (!workingDir.exists()) {
            workingDir.mkdirs();
        }

        // Create OCFL Storage dir
        final File ocflStorageDir = new File(workingDir, "ocfl");
        if (!ocflStorageDir.exists()) {
            ocflStorageDir.mkdirs();
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
        ObjectSource objectSource;
        InternalIDResolver idResolver;
        switch (f3SourceType) {
            case EXPORTED:
                Enforce.notNull(f3ExportedDir, "f3ExportDir must be used with EXPORTED source!");

                objectSource = new ArchiveExportedFoxmlDirectoryObjectSource(f3ExportedDir, null);
                break;
            case AKUBRA:
                Enforce.notNull(f3DatastreamsDir, "f3DatastreamsDir must be used with AKUBRA or LEGACY source!");
                Enforce.notNull(f3ObjectsDir, "f3ObjectsDir must be used with AKUBRA or LEGACY source!");
                Enforce.expressionTrue(f3ObjectsDir.exists(), f3ObjectsDir, "f3ObjectsDir must exist! " +
                        f3ObjectsDir.getAbsolutePath());

                idResolver = new AkubraFSIDResolver(f3DatastreamsDir);
                objectSource = new NativeFoxmlDirectoryObjectSource(f3ObjectsDir, idResolver, null);
                break;
            case LEGACY:
                Enforce.notNull(f3DatastreamsDir, "f3DatastreamsDir must be used with AKUBRA or LEGACY source!");
                Enforce.notNull(f3ObjectsDir, "f3ObjectsDir must be used with AKUBRA or LEGACY source!");
                Enforce.expressionTrue(f3ObjectsDir.exists(), f3ObjectsDir, "f3ObjectsDir must exist! " +
                        f3ObjectsDir.getAbsolutePath());

                idResolver = new LegacyFSIDResolver(f3DatastreamsDir);
                objectSource = new NativeFoxmlDirectoryObjectSource(f3ObjectsDir, idResolver, null);
                break;
            default:
                throw new RuntimeException("Should never happen");
        }

        // Build up the constituent parts of the 'migrator'
        final OCFLFedora4Client fedora4Client = new OCFLFedora4Client(
                ocflStorageDir.getAbsolutePath(),
                ocflStagingDir.getAbsolutePath(),
                ocflLayout);
        final OcflDriver ocflDriver = new HackyOcflDriver(fedora4Client);
        final FedoraObjectVersionHandler archiveGroupHandler = new ArchiveGroupHandler(ocflDriver);
        final FedoraObjectHandler versionHandler = new VersionAbstractionFedoraObjectHandler(archiveGroupHandler);
        final StreamingFedoraObjectHandler objectHandler = new ObjectAbstractionStreamingFedoraObjectHandler(
                versionHandler);

        // PID-list-managers (the second arg is "acceptAll". If resuming, we do not "acceptAll")
        final ResumePidListManager resumeManager = new ResumePidListManager(pidDir, !resume);
        final List<PidListManager> pidListManagerList = Collections.singletonList(resumeManager);

        final Migrator migrator = new Migrator();
        migrator.setLimit(objectLimit);
        migrator.setSource(objectSource);
        migrator.setHandler(objectHandler);
        migrator.setPidListManagers(pidListManagerList);

        try {
            migrator.run();
        } finally {
            fedora4Client.close();
            FileUtils.deleteDirectory(ocflStagingDir);
        }

        return 0;
    }

}
