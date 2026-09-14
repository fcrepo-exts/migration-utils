/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Directly exercises the {@link ObjectReference} and {@link ObjectVersionReference}
 * views produced by {@link ObjectAbstractionStreamingFedoraObjectHandler}.
 *
 * @author Dan Field
 */
@ExtendWith(MockitoExtension.class)
public class ObjectAbstractionStreamingFedoraObjectHandlerTest {

    @Mock
    private ObjectInfo objectInfo;

    @Mock
    private ObjectProperties objectProperties;

    private DatastreamVersion ds1;
    private DatastreamVersion ds2;

    @BeforeEach
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
                Assertions.assertSame(this.objectInfo, ref.getObjectInfo());
                Assertions.assertSame(this.objectProperties, ref.getObjectProperties());
                Assertions.assertEquals(List.of("DS1", "DS2"), ref.listDatastreamIds());
                Assertions.assertEquals("DS1.0", ref.getDatastreamVersions("DS1").get(0).getVersionId());

                // ObjectVersionReference view
                Assertions.assertSame(this.objectProperties, version.getObjectProperties());
            }
        };

        final var handler = new ObjectAbstractionStreamingFedoraObjectHandler(versionHandler);
        handler.beginObject(objectInfo);
        handler.processObjectProperties(objectProperties);
        handler.processDatastreamVersion(ds1);
        handler.processDatastreamVersion(ds2);
        handler.completeObject(objectInfo);

        Assertions.assertEquals(2, captured.size());

        final ObjectVersionReference first = captured.get(0);
        Assertions.assertEquals("2020-01-01T00:00:00.000Z", first.getVersionDate());
        Assertions.assertEquals(0, first.getVersionIndex());
        Assertions.assertTrue(first.isFirstVersion());
        Assertions.assertFalse(first.isLastVersion());
        Assertions.assertTrue(first.wasDatastreamChanged("DS1"));
        Assertions.assertFalse(first.wasDatastreamChanged("DS2"));

        final ObjectVersionReference last = captured.get(1);
        Assertions.assertEquals(1, last.getVersionIndex());
        Assertions.assertFalse(last.isFirstVersion());
        Assertions.assertTrue(last.isLastVersion());
        Assertions.assertTrue(last.wasDatastreamChanged("DS2"));
    }

    @Test
    public void abortClearsStateForReuse() {
        final FedoraObjectVersionHandler versionHandler = Mockito.mock(FedoraObjectVersionHandler.class);
        final var handler = new ObjectAbstractionStreamingFedoraObjectHandler(versionHandler);
        handler.beginObject(objectInfo);
        handler.processDatastreamVersion(ds1);
        handler.abortObject(objectInfo);

        Mockito.verifyNoInteractions(versionHandler);
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
