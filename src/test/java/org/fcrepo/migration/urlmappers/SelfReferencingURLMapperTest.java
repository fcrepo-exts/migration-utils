/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.urlmappers;

import org.fcrepo.migration.MigrationIDMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @author Dan Field
 */
@RunWith(MockitoJUnitRunner.class)
public class SelfReferencingURLMapperTest {

    private static final String SERVER = "localhost:8080";

    @Mock
    private MigrationIDMapper idMapper;

    private SelfReferencingURLMapper mapper;

    @Before
    public void setup() {
        Mockito.when(idMapper.getBaseURL()).thenReturn("http://fcrepo/rest");
        Mockito.when(idMapper.mapDatastreamPath(Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(i -> "/" + i.getArgument(0) + "/" + i.getArgument(1));
        mapper = new SelfReferencingURLMapper(SERVER, idMapper);
    }

    @Test
    public void testMapsOldContentUrl() {
        final String result = mapper.mapURL("http://localhost:8080/fedora/get/object:1/DS1");
        Assert.assertEquals("http://fcrepo/rest/object:1/DS1", result);
        Mockito.verify(idMapper).mapDatastreamPath("object:1", "DS1");
    }

    @Test
    public void testMapsNewContentUrl() {
        final String result =
                mapper.mapURL("http://localhost:8080/fedora/objects/object:1/datastreams/DS1/content");
        Assert.assertEquals("http://fcrepo/rest/object:1/DS1", result);
        Mockito.verify(idMapper).mapDatastreamPath("object:1", "DS1");
    }

    @Test
    public void testPassesThroughUnrelatedUrl() {
        final String url = "http://example.org/some/external/resource";
        Assert.assertEquals(url, mapper.mapURL(url));
        Mockito.verify(idMapper, Mockito.never()).mapDatastreamPath(Mockito.anyString(), Mockito.anyString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnUnhandledInternalUrl() {
        mapper.mapURL("http://localhost:8080/fedora/objects/object:1/unexpected");
    }
}
