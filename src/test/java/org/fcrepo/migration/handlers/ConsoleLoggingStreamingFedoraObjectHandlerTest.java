/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.handlers;

import java.util.Arrays;

import org.fcrepo.migration.DatastreamInfo;
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.ObjectInfo;
import org.fcrepo.migration.ObjectProperties;
import org.fcrepo.migration.ObjectProperty;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Exercises every callback of the console logging handler to ensure the
 * (side-effect only) logging paths run without error.
 *
 * @author Dan Field
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsoleLoggingStreamingFedoraObjectHandlerTest {

    @Mock
    private ObjectInfo objectInfo;

    @Mock
    private ObjectProperties objectProperties;

    @Mock
    private ObjectProperty objectProperty;

    @Mock
    private DatastreamVersion datastreamVersion;

    @Mock
    private DatastreamInfo datastreamInfo;

    private ConsoleLoggingStreamingFedoraObjectHandler handler;

    @Before
    public void setup() {
        handler = new ConsoleLoggingStreamingFedoraObjectHandler();
        Mockito.when(objectInfo.getPid()).thenReturn("object:1");
    }

    @Test
    public void testFullLifecycle() {
        Mockito.when(objectProperty.getName()).thenReturn("info:fedora/fedora-system:def/model#state");
        Mockito.when(objectProperty.getValue()).thenReturn("Active");
        Mockito.doReturn(Arrays.asList(objectProperty)).when(objectProperties).listProperties();
        Mockito.when(datastreamInfo.getDatastreamId()).thenReturn("DS1");
        Mockito.when(datastreamVersion.getDatastreamInfo()).thenReturn(datastreamInfo);
        Mockito.when(datastreamVersion.getVersionId()).thenReturn("DS1.0");

        handler.beginObject(objectInfo);
        handler.processObjectProperties(objectProperties);
        handler.processDatastreamVersion(datastreamVersion);
        handler.completeObject(objectInfo);
    }

    @Test
    public void testAbortObject() {
        handler.beginObject(objectInfo);
        handler.abortObject(objectInfo);
    }
}
