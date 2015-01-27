package org.fcrepo.migration.handlers;

import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.FedoraObjectHandler;
import org.fcrepo.migration.ObjectInfo;
import org.fcrepo.migration.ObjectProperties;
import org.fcrepo.migration.ObjectProperty;

/**
 * A simple FedoraObjectHandler implementation that simply outputs information
 * to the console.  This is likely only useful for debugging or testing other
 * code.
 */
public class ConsoleLoggingFedoraObjectHandler implements FedoraObjectHandler {

    private long start;

    @Override
    public void beginObject(ObjectInfo object) {
        start = System.currentTimeMillis();
        System.out.println(object.getPid());
    }

    @Override
    public void processObjectProperties(ObjectProperties properties) {
        System.out.println("  Properties");
        for (ObjectProperty p : properties.listProperties()) {
            System.out.println("    " + p.getName() + " = " + p.getValue());
        }
    }

    @Override
    public void processDatastreamVersion(DatastreamVersion dsVersion) {
        System.out.println("  " + dsVersion.getDatastreamInfo().getDatastreamId() + " version "
                + dsVersion.getVersionId());
    }

    @Override
    public void completeObject(ObjectInfo object) {
        System.out.println(object.getPid() + " parsed in " + (System.currentTimeMillis() - start) + "ms.");
    }

    @Override
    public void abortObject(ObjectInfo object) {
        System.out.println(object.getPid() + " failed to parse in " + (System.currentTimeMillis() - start) + "ms.");
    }
}
