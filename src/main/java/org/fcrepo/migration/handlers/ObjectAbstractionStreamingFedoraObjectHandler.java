package org.fcrepo.migration.handlers;

import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.FedoraObjectHandler;
import org.fcrepo.migration.ObjectInfo;
import org.fcrepo.migration.ObjectProperties;
import org.fcrepo.migration.ObjectReference;
import org.fcrepo.migration.StreamingFedoraObjectHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A StreamingFedoraObjectHandler implementation that caches all the references to 
 * the Fedora 3 object and provides them to a FedoraObjectHandler implementation 
 * which in turn can process the object as a whole in a random-access fashion rather
 * than as a stream. 
 */
public class ObjectAbstractionStreamingFedoraObjectHandler implements StreamingFedoraObjectHandler {
    
    private FedoraObjectHandler handler;
    
    private ObjectInfo objectInfo;
    
    private ObjectProperties objectProperties;
    
    private List<String> dsIds;
    
    private Map<String, List<DatastreamVersion>> dsIdToVersionListMap;

    public ObjectAbstractionStreamingFedoraObjectHandler(FedoraObjectHandler objectHandler) {
        this.handler = objectHandler;
        this.dsIds = new ArrayList<String>();
        this.dsIdToVersionListMap = new HashMap<String, List<DatastreamVersion>>();
    }
    
    @Override
    public void beginObject(ObjectInfo object) {
        this.objectInfo = object;
    }

    @Override
    public void processObjectProperties(ObjectProperties properties) {
        this.objectProperties = properties;
    }

    @Override
    public void processDatastreamVersion(DatastreamVersion dsVersion) {
        List<DatastreamVersion> versions = dsIdToVersionListMap.get(dsVersion.getDatastreamInfo().getDatastreamId());
        if (versions == null) {
            dsIds.add(dsVersion.getDatastreamInfo().getDatastreamId());
            versions = new ArrayList<DatastreamVersion>();
            dsIdToVersionListMap.put(dsVersion.getDatastreamInfo().getDatastreamId(), versions);
        }
        versions.add(dsVersion);
    }

    @Override
    public void completeObject(ObjectInfo object) {
        handler.processObject(new ObjectReference() {
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
            public List<DatastreamVersion> getDatastreamVersions(String datastreamId) {
                return dsIdToVersionListMap.get(datastreamId);
            }
        });
    }

    @Override
    public void abortObject(ObjectInfo object) {
    }
}
