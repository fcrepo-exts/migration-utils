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

import org.fcrepo.migration.pidlist.ResumePidListManager;
import org.fcrepo.migration.pidlist.UserProvidedPidListManager;
import org.slf4j.Logger;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;

import javax.xml.stream.XMLStreamException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * A class that represents a command-line program to migrate a fedora 3
 * repository to fedora 4.
 *
 * There are two main configuration options: the source and the handler.
 *
 * The source is responsible for exposing objects from a fedora repository,
 * while the handler is responsible for processing each one.
 * @author mdurbin
 */
public class Migrator {

    private static final Logger LOGGER = getLogger(Migrator.class);

    /**
     * the main method.
     * @param args the arguments
     * @throws IOException IO exception
     * @throws XMLStreamException xml stream exception
     */
    public static void main(final String [] args) throws IOException, XMLStreamException {
        // Single arg with path to properties file is required
        if (args.length != 1) {
            printHelp();
            return;
        }

        final ConfigurableApplicationContext context = new FileSystemXmlApplicationContext(args[0]);
        final Migrator m = context.getBean("migrator", Migrator.class);
        try {
            m.run();
        } finally {
            context.close();
        }
    }

    private ObjectSource source;

    private StreamingFedoraObjectHandler handler;

    private int limit;

    private ResumePidListManager resumePidListManager;
    private UserProvidedPidListManager userProvidedPidListManager;

    private boolean continueOnError;

    /**
     * the migrator. set limit to -1.
     */
    public Migrator() {
        limit = -1;
    }

    /**
     * set the limit.
     * @param limit the limit
     */
    public void setLimit(final int limit) {
        this.limit = limit;
    }

    /**
     * set the source.
     * @param source the object source
     */
    public void setSource(final ObjectSource source) {
        this.source = source;
    }


    /**
     * set the handler.
     * @param handler the handler
     */
    public void setHandler(final StreamingFedoraObjectHandler handler) {
        this.handler = handler;
    }

    /**
     * set UserProvidedPidListManager
     *
     * @param manager the list
     */
    public void setUserProvidedPidListManager(final UserProvidedPidListManager manager) {
        this.userProvidedPidListManager = manager;
    }

    public void setResumePidListManager(final ResumePidListManager manager) {
        this.resumePidListManager = manager;
    }

    /**
     * set the continue on error flag
     *
     * @param flag flag indicating whether or not to continue on error.
     */
    public void setContinueOnError(final boolean flag) {
        this.continueOnError = flag;
    }

    /**
     * The constructor for migrator.
     * @param source the source
     * @param handler the handler
     */
    public Migrator(final ObjectSource source, final StreamingFedoraObjectHandler handler) {
        this();
        this.source = source;
        this.handler = handler;
    }

    /**
     * the run method for migrator.
     *
     * @throws XMLStreamException xml stream exception
     */
    public void run() throws XMLStreamException {
        int index = 0;
        int acceptedPids = 0;

        for (final var iterator = source.iterator(); iterator.hasNext();) {
            try (final var o = iterator.next()) {
                final String pid = o.getObjectInfo().getPid();
                if (pid != null) {
                    // Process if limit is '-1', or we have not hit the non-negative 'limit'...
                    if (!(limit < 0 || index++ < limit)) {
                        LOGGER.info("Reached processing limit {}", limit);
                        break;
                    }

                    if (acceptPid(pid)) {
                        LOGGER.info("Processing \"" + pid + "\"...");
                        acceptedPids += 1;
                        try {
                            o.processObject(handler);
                        } catch (Exception ex) {
                            final var message = String.format("MIGRATION_FAILURE: pid=\"%s\", message=\"%s\"",
                                    pid, ex.getMessage());

                            if (this.continueOnError) {
                                LOGGER.error(message, ex);
                            } else {
                                throw new RuntimeException(message, ex);
                            }
                        }
                    }
                    if (userProvidedPidListManager != null &&
                            userProvidedPidListManager.finishedProcessingAllPids(acceptedPids)) {
                        LOGGER.info("finished processing everything in pidlist - exiting.");
                        return;
                    }
                }
            } catch (Exception ex) {
                final var message = String.format("MIGRATION_FAILURE: UNREADABLE_OBJECT: message=\"%s\"",
                        ex.getMessage());

                if (this.continueOnError) {
                    LOGGER.error(message, ex);
                } else {
                    throw new RuntimeException(message, ex);
                }
            }
        }
    }

    private boolean acceptPid(final String pid) {
        // If any manager DOES NOT accept the PID, return false
        if (resumePidListManager != null && !resumePidListManager.accept(pid)) {
            return false;
        }
        if (userProvidedPidListManager != null && !userProvidedPidListManager.accept(pid)) {
            return false;
        }

        return true;
    }

    private static void printHelp() throws IOException {
        final StringBuilder sb = new StringBuilder();
        sb.append("============================\n");
        sb.append("Please provide the directory path to a configuration file!");
        sb.append("\n");
        sb.append("See: https://github.com/fcrepo-exts/migration-utils/blob/master/");
        sb.append("src/main/resources/spring/migration-bean.xml");
        sb.append("\n\n");
        sb.append("The configuration file should contain the following (with appropriate values):");
        sb.append("\n");
        sb.append("~~~~~~~~~~~~~~\n");

        final ClassPathResource resource = new ClassPathResource("spring/migration-bean.xml");
        try (final InputStream example = resource.getInputStream();
             final BufferedReader reader = new BufferedReader(new InputStreamReader(example))) {
            String line = reader.readLine();
            while (null != line) {
                sb.append(line);
                sb.append("\n");
                line = reader.readLine();
            }

            sb.append("~~~~~~~~~~~~~~\n\n");
            sb.append("See top of this output for details.\n");
            sb.append("============================\n");
            System.out.println(sb.toString());
        }
    }
}
