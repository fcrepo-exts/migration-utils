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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.FedoraObjectHandler;
import org.fcrepo.migration.FedoraObjectVersionHandler;
import org.fcrepo.migration.ObjectInfo;
import org.fcrepo.migration.ObjectProperties;
import org.fcrepo.migration.ObjectReference;
import org.fcrepo.migration.ObjectVersionReference;

/**
 * A FedoraObjectHandler implementation that analyzes the ObjectReference provided
 * to the processObject method and exposes the version abstraction to a wrapped
 * FedoraObjectVersionHandler implementation.
 * @author mdurbin
 */
public class VersionAbstractionFedoraObjectHandler implements FedoraObjectHandler {

    private FedoraObjectVersionHandler handler;

    /**
     * version abstraction fedora object handler.
     * @param versionHandler the version handler
     */
    public VersionAbstractionFedoraObjectHandler(final FedoraObjectVersionHandler versionHandler) {
        this.handler = versionHandler;
    }

    @Override
    public void processObject(final ObjectReference object) {
        final Map<String, List<DatastreamVersion>> versionMap = buildVersionMap(object);
        final List<String> versionDates = new ArrayList<String>(versionMap.keySet());
        Collections.sort(versionDates);
        final List<ObjectVersionReference> versions = new ArrayList<ObjectVersionReference>();
        for (final String versionDate : versionDates) {
            versions.add(new ObjectVersionReference() {
                @Override
                public ObjectReference getObject() {
                    return object;
                }

                @Override
                public ObjectInfo getObjectInfo() {
                    return object.getObjectInfo();
                }

                @Override
                public ObjectProperties getObjectProperties() {
                    return object.getObjectProperties();
                }

                @Override
                public String getVersionDate() {
                    return versionDate;
                }

                @Override
                public List<DatastreamVersion> listChangedDatastreams() {
                    return versionMap.get(versionDate);
                }

                @Override
                public boolean isLastVersion() {
                    return versionDates.get(versionDates.size() - 1).equals(versionDate);
                }

                @Override
                public boolean isFirstVersion() {
                    return versionDates.get(0).equals(versionDate);
                }

                @Override
                public int getVersionIndex() {
                    return versionDates.indexOf(versionDate);
                }

                @Override
                public boolean wasDatastreamChanged(final String dsId) {
                    for (final DatastreamVersion v : listChangedDatastreams()) {
                        if (v.getDatastreamInfo().getDatastreamId().equals(dsId)) {
                            return true;
                        }
                    }
                    return false;
                }

            });
        }
        handler.processObjectVersions(versions);

    }

    private Map<String, List<DatastreamVersion>> buildVersionMap(final ObjectReference object) {
        final Map<String, List<DatastreamVersion>> versionMap = new HashMap<String, List<DatastreamVersion>>();
        for (final String dsId : object.listDatastreamIds()) {
            for (final DatastreamVersion v : object.getDatastreamVersions(dsId)) {
                final String date = v.getCreated();
                List<DatastreamVersion> versionsForDate = versionMap.get(date);
                if (versionsForDate == null) {
                    versionsForDate = new ArrayList<DatastreamVersion>();
                    versionMap.put(date, versionsForDate);
                }
                versionsForDate.add(v);
            }
        }
        return versionMap;
    }
}
