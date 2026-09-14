/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.migration.urlmappers;

import org.fcrepo.migration.MigrationIDMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * @author Dan Field
 */
@ExtendWith(MockitoExtension.class)
public class SelfReferencingURLMapperTest {

    private static final String SERVER = "localhost:8080";

    @Mock
    private MigrationIDMapper idMapper;

    private SelfReferencingURLMapper mapper;

    @BeforeEach
    public void setup() {
        // Shared by the URL-mapping tests; the pass-through and error cases never reach the mapper.
        Mockito.lenient().when(idMapper.getBaseURL()).thenReturn("http://fcrepo/rest");
        Mockito.lenient().when(idMapper.mapDatastreamPath(Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(i -> "/" + i.getArgument(0) + "/" + i.getArgument(1));
        mapper = new SelfReferencingURLMapper(SERVER, idMapper);
    }

    @Test
    public void testMapsOldContentUrl() {
        final String result = mapper.mapURL("http://localhost:8080/fedora/get/object:1/DS1");
        Assertions.assertEquals("http://fcrepo/rest/object:1/DS1", result);
        Mockito.verify(idMapper).mapDatastreamPath("object:1", "DS1");
    }

    @Test
    public void testMapsNewContentUrl() {
        final String result =
                mapper.mapURL("http://localhost:8080/fedora/objects/object:1/datastreams/DS1/content");
        Assertions.assertEquals("http://fcrepo/rest/object:1/DS1", result);
        Mockito.verify(idMapper).mapDatastreamPath("object:1", "DS1");
    }

    @Test
    public void testPassesThroughUnrelatedUrl() {
        final String url = "http://example.org/some/external/resource";
        Assertions.assertEquals(url, mapper.mapURL(url));
        Mockito.verify(idMapper, Mockito.never()).mapDatastreamPath(Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void testThrowsOnUnhandledInternalUrl() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                mapper.mapURL("http://localhost:8080/fedora/objects/object:1/unexpected"));
    }
}
