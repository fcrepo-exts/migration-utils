package org.fcrepo.migration.handlers;

import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.FedoraObjectHandler;
import org.fcrepo.migration.FedoraObjectVersionHandler;
import org.fcrepo.migration.ObjectInfo;
import org.fcrepo.migration.ObjectProperties;
import org.fcrepo.migration.ObjectReference;
import org.fcrepo.migration.ObjectVersionReference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A FedoraObjectHandler implementation that analyzes the ObjectReference provided
 * to the processObject method and exposes the version abstraction to a wrapped
 * FedoraObjectVersionHandler implementation.
 */
public class VersionAbstractionFedoraObjectHandler implements FedoraObjectHandler {

    private FedoraObjectVersionHandler handler;

    public VersionAbstractionFedoraObjectHandler(FedoraObjectVersionHandler versionHandler) {
        this.handler = versionHandler;
    }
    
    @Override
    public void processObject(final ObjectReference object) {
        final Map<String, List<DatastreamVersion>> versionMap = buildVersionMap(object);
        List<String> versionDates = new ArrayList<String>(versionMap.keySet());
        Collections.sort(versionDates);
        for (final String versionDate : versionDates) {
            ObjectVersionReference ref = new ObjectVersionReference() {
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

            };
            handler.processObjectVersion(ref);
        }
        
    }
    
    private Map<String, List<DatastreamVersion>> buildVersionMap(ObjectReference object) {
        Map<String, List<DatastreamVersion>> versionMap = new HashMap<String, List<DatastreamVersion>>();
        for (String dsId : object.listDatastreamIds()) {
            for (DatastreamVersion v : object.getDatastreamVersions(dsId)) {
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
