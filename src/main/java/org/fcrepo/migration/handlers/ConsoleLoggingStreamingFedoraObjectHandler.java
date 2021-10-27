/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
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
    public void completeObject(final ObjectInfo object) {
        System.out.println(object.getPid() + " parsed in " + (System.currentTimeMillis() - start) + "ms.");
    }

    @Override
    public void abortObject(final ObjectInfo object) {
        System.out.println(object.getPid() + " failed to parse in " + (System.currentTimeMillis() - start) + "ms.");
    }
}
