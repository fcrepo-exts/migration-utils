/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.handlers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.fcrepo.migration.DatastreamInfo;
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.FedoraObjectVersionHandler;
import org.fcrepo.migration.ObjectInfo;
import org.fcrepo.migration.ObjectProperties;
import org.fcrepo.migration.ObjectReference;
import org.fcrepo.migration.ObjectVersionReference;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Directly exercises the {@link ObjectReference} and {@link ObjectVersionReference}
 * views produced by {@link ObjectAbstractionStreamingFedoraObjectHandler}.
 *
 * @author Claude
 */
@RunWith(MockitoJUnitRunner.class)
public class ObjectAbstractionStreamingFedoraObjectHandlerTest {

    @Mock
    private ObjectInfo objectInfo;

    @Mock
    private ObjectProperties objectProperties;

    private DatastreamVersion ds1;
    private DatastreamVersion ds2;

    @Before
    public void setup() {
        ds1 = datastreamVersion("DS1", "DS1.0", "2020-01-01T00:00:00.000Z", Instant.parse("2020-01-01T00:00:00Z"));
        ds2 = datastreamVersion("DS2", "DS2.0", "2020-01-02T00:00:00.000Z", Instant.parse("2020-01-02T00:00:00Z"));
    }

    @Test
    public void exercisesReferenceViews() {
        final List<ObjectVersionReference> captured = new ArrayList<>();
        final FedoraObjectVersionHandler versionHandler = (versions, objectInfo) -> {
            for (final ObjectVersionReference version : versions) {
                captured.add(version);

                // ObjectReference view
                final ObjectReference ref = version.getObject();
                Assert.assertSame(this.objectInfo, ref.getObjectInfo());
                Assert.assertSame(this.objectProperties, ref.getObjectProperties());
                Assert.assertEquals(List.of("DS1", "DS2"), ref.listDatastreamIds());
                Assert.assertEquals("DS1.0", ref.getDatastreamVersions("DS1").get(0).getVersionId());

                // ObjectVersionReference view
                Assert.assertSame(this.objectProperties, version.getObjectProperties());
            }
        };

        final var handler = new ObjectAbstractionStreamingFedoraObjectHandler(versionHandler);
        handler.beginObject(objectInfo);
        handler.processObjectProperties(objectProperties);
        handler.processDatastreamVersion(ds1);
        handler.processDatastreamVersion(ds2);
        handler.completeObject(objectInfo);

        Assert.assertEquals(2, captured.size());

        final ObjectVersionReference first = captured.get(0);
        Assert.assertEquals("2020-01-01T00:00:00.000Z", first.getVersionDate());
        Assert.assertEquals(0, first.getVersionIndex());
        Assert.assertTrue(first.isFirstVersion());
        Assert.assertFalse(first.isLastVersion());
        Assert.assertTrue(first.wasDatastreamChanged("DS1"));
        Assert.assertFalse(first.wasDatastreamChanged("DS2"));

        final ObjectVersionReference last = captured.get(1);
        Assert.assertEquals(1, last.getVersionIndex());
        Assert.assertFalse(last.isFirstVersion());
        Assert.assertTrue(last.isLastVersion());
        Assert.assertTrue(last.wasDatastreamChanged("DS2"));
    }

    @Test
    public void abortClearsStateForReuse() {
        final FedoraObjectVersionHandler versionHandler = Mockito.mock(FedoraObjectVersionHandler.class);
        final var handler = new ObjectAbstractionStreamingFedoraObjectHandler(versionHandler);
        handler.beginObject(objectInfo);
        handler.processDatastreamVersion(ds1);
        handler.abortObject(objectInfo);

        Mockito.verifyZeroInteractions(versionHandler);
    }

    private DatastreamVersion datastreamVersion(final String dsId, final String versionId,
                                                final String created, final Instant createdInstant) {
        final DatastreamInfo info = Mockito.mock(DatastreamInfo.class);
        Mockito.lenient().when(info.getDatastreamId()).thenReturn(dsId);
        final DatastreamVersion version = Mockito.mock(DatastreamVersion.class);
        Mockito.lenient().when(version.getDatastreamInfo()).thenReturn(info);
        Mockito.lenient().when(version.getVersionId()).thenReturn(versionId);
        Mockito.lenient().when(version.getCreated()).thenReturn(created);
        Mockito.lenient().when(version.getCreatedInstant()).thenReturn(createdInstant);
        return version;
    }
}
