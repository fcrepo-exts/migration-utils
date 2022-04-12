/*
 * Copyright 2019 DuraSpace, Inc.
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


/**
 * Control was pids/datastreams to accept only the HEAD versions of datastreams.
 *
 * If headOnly is true and there are no pids specified, all datastreams are accepted. Otherwise, pids and their
 * datastreams can be specified by passing in a File with a listing of the pids/datastreams to accept.
 *
 * @author mikejritter
 */
public class HeadOnlyPidListManager {

    private final boolean headOnly;
    private final Set<String> pidList = new HashSet<>();

    public HeadOnlyPidListManager(final boolean headOnly) {
        this.headOnly = headOnly;
    }

    public HeadOnlyPidListManager(final boolean headOnly, final File headOnlyListFile) {
        this.headOnly = headOnly;
        if (headOnly && headOnlyListFile != null) {
            if (!headOnlyListFile.exists() || !headOnlyListFile.canRead()) {
                throw new IllegalArgumentException("File either does not exist or is inaccessible :" +
                                                   headOnlyListFile.getAbsolutePath());
            }

            try (BufferedReader reader = new BufferedReader(new FileReader(headOnlyListFile))) {
                reader.lines().forEach(pidList::add);
            } catch (IOException e) {
                // Should not happen based on previous check
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Similar to accept from {@link PidListManager}, but with an additional variable for the datastreamId. If no
     * entries are in the pidList, it is assumed all objects should be accepted.
     *
     * @param pid the pid of the object
     * @param dsId the datastreamId
     * @return true if Object should be processed
     */
    public boolean accept(final String pid, final String dsId) {
        final var fullDsId = pid + "/" + dsId;
        final var accept = pidList.isEmpty() || pidList.contains(pid) || pidList.contains(fullDsId);
        return headOnly && accept;
    }
}
