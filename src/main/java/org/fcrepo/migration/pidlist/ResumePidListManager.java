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
package org.fcrepo.migration.pidlist;

import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * This class "accepts" PIDs that have not already been migrated.
 * <p>
 * The approach taking by this implementation is to record:
 * - the number of objects that have been migrated, and
 * - the PID of the last migrated object
 * <p>
 * The assumption is that the order of processed PIDs/Objects is deterministic
 *
 * @author awoods
 * @since 2019-11-08
 */
public class ResumePidListManager implements PidListManager {

    private static final Logger LOGGER = getLogger(ResumePidListManager.class);

    private File resumeFile;

    // Accept all PIDs, even if they have been processed before
    private boolean acceptAll;

    // Position of the last processed PID/Object (assuming deterministic ordering)
    private int pidResumeIndex;

    // Value of the last processed PID/Object
    private String pidResumeValue;

    // Number of times "accept" has been called
    private int index = 0;

    // Last value of current PID
    private String value = "foo";


    /**
     * Constructor
     *
     * @param pidDir where resume file will be read/created
     * @param acceptAll whether to process all pids even if they've been processed before.
     */
    public ResumePidListManager(final File pidDir, final boolean acceptAll) {
        if (!pidDir.exists()) {
            pidDir.mkdirs();
        }

        if (!pidDir.isDirectory()) {
            throw new IllegalArgumentException("Arg must be a directory: " + pidDir.getAbsolutePath());
        }

        this.acceptAll = acceptAll;
        this.resumeFile = new File(pidDir, "resume.txt");
        LOGGER.debug("Resume pid file: {}, accept all? {}", resumeFile.getAbsolutePath(), acceptAll);

        try {
            // Load pidResumeIndex and pidResumeValue
            loadResumeFile();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadResumeFile() throws IOException {

        // First run? file does not yet exist?
        if (!resumeFile.exists() || resumeFile.length() == 0) {
            updateResumeFile(value, index);
        }

        try (final BufferedReader reader = new BufferedReader(new FileReader(resumeFile))) {

            // First line contains PID
            pidResumeValue = reader.readLine();

            // Second line contains index
            pidResumeIndex = Integer.parseInt(reader.readLine());
        }
    }


    /**
     * This method
     * - returns false if "accept" has been called less than pidResumeIndex times
     * - returns true if "accept" has been called
     */
    @Override
    public boolean accept(final String pid) {
        final String logMsg = "PID: " + pid + ", accept? ";

        final String previousValue = value;
        value = pid;
        index++;

        // Do not accept.. the previous run index is higher
        if (index - 1 < pidResumeIndex) {

            // Are we accepting all?
            LOGGER.debug(logMsg + acceptAll);
            return acceptAll;
        }

        // We are at the first PID that has not previously been processed
        if (index - 1 == pidResumeIndex) {

            // index matches, but value DOES NOT match the last state of previous run!
            if (!previousValue.equalsIgnoreCase(pidResumeValue)) {
                final String msg = "Number of accept requests does not align with expected PID value! " +
                        "index: " + index + ", " +
                        "pid: " + pid + ", " +
                        "expected pid: " + pidResumeValue;
                throw new IllegalStateException(msg);
            }
        }

        // New "accept" requests
        updateResumeFile(value, index);

        LOGGER.debug(logMsg + true);
        return true;
    }

    /**
     * This method resets the current index and value, and resets the resume file
     * -- Used for test --
     */
    void reset() {
        index = 0;
        value = "foo";
        updateResumeFile(value, index);
    }

    private void updateResumeFile(final String pid, final int index) {
        // Create writer of resumeFile (expense to do everytime... but need to overwrite file)
        PrintWriter resumeFileWriter = null;
        try {
            resumeFileWriter = new PrintWriter(new FileWriter(resumeFile, false));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        resumeFileWriter.write(pid);
        resumeFileWriter.write(System.getProperty("line.separator"));
        resumeFileWriter.write(Integer.toString(index));

        resumeFileWriter.flush();
        resumeFileWriter.close();
    }
}
