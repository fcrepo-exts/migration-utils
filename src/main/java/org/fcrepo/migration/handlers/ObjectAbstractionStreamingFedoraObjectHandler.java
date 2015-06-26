package org.fcrepo.migration.handlers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.FedoraObjectHandler;
import org.fcrepo.migration.ObjectInfo;
import org.fcrepo.migration.ObjectProperties;
import org.fcrepo.migration.ObjectReference;
import org.fcrepo.migration.StreamingFedoraObjectHandler;

/**
 * A StreamingFedoraObjectHandler implementation that caches all the references to
 * the Fedora 3 object and provides them to a FedoraObjectHandler implementation
 * which in turn can process the object as a whole in a random-access fashion rather
 * than as a stream.
 * @author mdurbin
 */
public class ObjectAbstractionStreamingFedoraObjectHandler implements StreamingFedoraObjectHandler {

    private FedoraObjectHandler handler;

    private ObjectInfo objectInfo;

    private ObjectProperties objectProperties;

    private List<String> dsIds;

    private Map<String, List<DatastreamVersion>> dsIdToVersionListMap;

    private int disseminatorsSkipped = 0;

    /**
     * the object abstraction streaming fedora object handler.
     * @param objectHandler the fedora object handler
     */
    public ObjectAbstractionStreamingFedoraObjectHandler(final FedoraObjectHandler objectHandler) {
        this.handler = objectHandler;
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
    public void processDisseminator() {
        disseminatorsSkipped ++;
    }

    @Override
    public void completeObject(final ObjectInfo object) {
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
            public List<DatastreamVersion> getDatastreamVersions(final String datastreamId) {
                return dsIdToVersionListMap.get(datastreamId);
            }

            @Override
            public boolean hadFedora2Disseminators() {
                return disseminatorsSkipped > 0;
            }
        });
        cleanForReuse();
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
}
