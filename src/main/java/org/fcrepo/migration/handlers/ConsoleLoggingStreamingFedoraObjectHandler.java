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
package org.fcrepo.migration.handlers;

import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.ObjectInfo;
import org.fcrepo.migration.ObjectProperties;
import org.fcrepo.migration.ObjectProperty;
import org.fcrepo.migration.StreamingFedoraObjectHandler;

/**
 * A simple StreamingFedoraObjectHandler implementation that simply outputs information
 * to the console.  This is likely only useful for debugging or testing other
 * code.
 * @author mdurbin
 */
public class ConsoleLoggingStreamingFedoraObjectHandler implements StreamingFedoraObjectHandler {

    private long start;

    @Override
    public void beginObject(final ObjectInfo object) {
        start = System.currentTimeMillis();
        System.out.println(object.getPid());
    }

    @Override
    public void processObjectProperties(final ObjectProperties properties) {
        System.out.println("  Properties");
        for (final ObjectProperty p : properties.listProperties()) {
            System.out.println("    " + p.getName() + " = " + p.getValue());
        }
    }

    @Override
    public void processDatastreamVersion(final DatastreamVersion dsVersion) {
        System.out.println("  " + dsVersion.getDatastreamInfo().getDatastreamId() + " version "
                + dsVersion.getVersionId());
    }

    @Override
    public void processDisseminator() {
        System.out.println("  DISSEMINATOR found and skipped!");
    }

    @Override
    public void completeObject(final ObjectInfo object) {
        System.out.println(object.getPid() + " parsed in " + (System.currentTimeMillis() - start) + "ms.");
    }

    @Override
    public void abortObject(final ObjectInfo object) {
        System.out.println(object.getPid() + " failed to parse in " + (System.currentTimeMillis() - start) + "ms.");
    }
}
