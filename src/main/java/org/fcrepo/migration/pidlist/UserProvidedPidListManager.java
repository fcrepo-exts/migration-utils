package org.fcrepo.migration.pidlist;

import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * This class "accepts" and PIDs that are included in the user-provided list
 *
 * @author awoods
 * @since 2019-11-08
 */
public class UserProvidedPidListManager implements PidListManager {

    private static final Logger LOGGER = getLogger(UserProvidedPidListManager.class);

    private Set<String> pidList = new HashSet<>();

    /**
     * Constructor
     *
     * @param pidListFile provided by user
     */
    public UserProvidedPidListManager(final File pidListFile) {

        // If arg is null, we will accept all PIDs
        if (pidListFile != null) {
            if (!pidListFile.exists() || !pidListFile.canRead()) {
                throw new IllegalArgumentException("File either does not exist or is inaccessible :" +
                        pidListFile.getAbsolutePath());
            }

            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(pidListFile));
            } catch (FileNotFoundException e) {
                // Should not happen based on previous check
                throw new RuntimeException(e);
            }

            reader.lines().forEach(l -> pidList.add(l));
        }
    }


    @Override
    public boolean accept(final String pid) {
        final boolean doAccept =  pidList.isEmpty() || pidList.contains(pid);
        LOGGER.debug("PID: {}, accept? {}", pid, doAccept);
        return doAccept;
    }
}
