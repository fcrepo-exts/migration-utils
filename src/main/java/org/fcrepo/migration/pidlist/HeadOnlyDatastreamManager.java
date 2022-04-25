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
 * Control was datastreams to accept only the HEAD versions of datastreams.
 *
 * If headOnly is true and there are no datastreams specified, all datastreams are accepted. Otherwise,
 * datastreams can be specified by passing in a File with a listing of the datastream ids to accept.
 *
 * @author mikejritter
 */
public class HeadOnlyDatastreamManager {

    private final boolean headOnly;
    private final Set<String> dsIdList = new HashSet<>();

    public HeadOnlyDatastreamManager(final boolean headOnly) {
        this.headOnly = headOnly;
    }

    public HeadOnlyDatastreamManager(final boolean headOnly, final File headOnlyListFile) {
        this.headOnly = headOnly;
        if (headOnly && headOnlyListFile != null) {
            if (!headOnlyListFile.exists() || !headOnlyListFile.canRead()) {
                throw new IllegalArgumentException("File either does not exist or is inaccessible :" +
                                                   headOnlyListFile.getAbsolutePath());
            }

            try (BufferedReader reader = new BufferedReader(new FileReader(headOnlyListFile))) {
                reader.lines().forEach(dsIdList::add);
            } catch (IOException e) {
                // Should not happen based on previous check
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Similar to accept from {@link PidListManager}, but for the datastreamId. If no entries are present,
     * it is assumed all objects should be accepted.
     *
     * @param dsId the datastreamId
     * @return true if Object is required to migrate only HEAD datastreams
     */
    public boolean accept(final String dsId) {
        final var accept = dsIdList.isEmpty() || dsIdList.contains(dsId);
        return headOnly && accept;
    }
}
