/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.handlers;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;

import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.ObjectInfo;
import org.fcrepo.migration.ObjectProperties;
import org.fcrepo.migration.ObjectReference;
import org.fcrepo.migration.ObjectVersionReference;
import org.fcrepo.migration.FedoraObjectVersionHandler;
import org.fcrepo.migration.StreamingFedoraObjectHandler;

/**
 * A StreamingFedoraObjectHandler implementation that caches all the references to
 * the Fedora 3 object and provides them to a FedoraObjectHandler implementation
 * which in turn can process the object as a whole in a random-access fashion rather
 * than as a stream.
 * @author mdurbin
 */
public class ObjectAbstractionStreamingFedoraObjectHandler implements StreamingFedoraObjectHandler {

    private FedoraObjectVersionHandler versionHandler;

    private ObjectInfo objectInfo;

    private ObjectProperties objectProperties;

    private List<String> dsIds;

    private Map<String, List<DatastreamVersion>> dsIdToVersionListMap;

    /**
     * the object abstraction streaming fedora object handler.
     * @param versionHandler the fedora object version handler
     */
    public ObjectAbstractionStreamingFedoraObjectHandler(final FedoraObjectVersionHandler versionHandler) {
        this.versionHandler = versionHandler;
        this.dsIds = new ArrayList<String>();
        this.dsIdToVersionListMap = new HashMap<String, List<DatastreamVersion>>();
    }

    @Override
    public void beginObject(final ObjectInfo object) {
        this.objectInfo = object;
    }

    @Override
    public void processObjectProperties(final ObjectProperties properties) {
        this.objectProperties = properties;
    }

    @Override
    public void processDatastreamVersion(final DatastreamVersion dsVersion) {
        List<DatastreamVersion> versions = dsIdToVersionListMap.get(dsVersion.getDatastreamInfo().getDatastreamId());
        if (versions == null) {
            dsIds.add(dsVersion.getDatastreamInfo().getDatastreamId());
            versions = new ArrayList<DatastreamVersion>();
            dsIdToVersionListMap.put(dsVersion.getDatastreamInfo().getDatastreamId(), versions);
        }
        versions.add(dsVersion);
    }

    @Override
    public void completeObject(final ObjectInfo object) {
        final var objectReference = getObjectReference();
        try {
            final Map<String, List<DatastreamVersion>> versionMap = buildVersionMap();
            final List<String> versionDates = new ArrayList<String>(versionMap.keySet());
            Collections.sort(versionDates);
            final List<ObjectVersionReference> versions = new ArrayList<ObjectVersionReference>();
            for (final String versionDate : versionDates) {
                versions.add(getObjectVersionReference(versionDate, versionDates, objectReference, versionMap));
            }

            // Need to sort the datastream versions because they may not appear in order in FOXML
            dsIdToVersionListMap.forEach((dsId, dsVersions) -> {
                dsVersions.sort(Comparator.comparing(DatastreamVersion::getCreatedInstant));
            });

            versionHandler.processObjectVersions(versions, object);
        } finally {
            cleanForReuse();
        }
    }

    @Override
    public void abortObject(final ObjectInfo object) {
        cleanForReuse();
    }

    /**
     * Removes any state that's specific to a Fedora 3 object that was processed
     * so that this Handler may be reused for a different object.
     */
    private void cleanForReuse() {
        this.dsIds.clear();
        this.dsIdToVersionListMap.clear();
    }

    private Map<String, List<DatastreamVersion>> buildVersionMap() {
        final Map<String, List<DatastreamVersion>> versionMap = new HashMap<String, List<DatastreamVersion>>();
        for (final String dsId : dsIds) {
            for (final DatastreamVersion v : dsIdToVersionListMap.get(dsId)) {
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

    private ObjectReference getObjectReference() {
        return new ObjectReference() {
            @Override
            public ObjectInfo getObjectInfo() {
                return objectInfo;
            }

            @Override
            public ObjectProperties getObjectProperties() {
                return objectProperties;
            }

            @Override
            public List<String> listDatastreamIds() {
                return dsIds;
            }

            @Override
            public List<DatastreamVersion> getDatastreamVersions(final String datastreamId) {
                return dsIdToVersionListMap.get(datastreamId);
            }
        };
    }

    private ObjectVersionReference getObjectVersionReference(final String versionDate, final List<String> versionDates,
                                                             final ObjectReference objectReference,
                                                             final Map<String, List<DatastreamVersion>> versionMap) {
        return new ObjectVersionReference() {
            @Override
            public ObjectReference getObject() {
                return objectReference;
            }

            @Override
            public ObjectProperties getObjectProperties() {
                return objectProperties;
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
        };
    }
}
