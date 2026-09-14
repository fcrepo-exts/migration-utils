/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.foxml;

import java.io.InputStream;
import java.util.List;

import jakarta.xml.bind.JAXBException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 *
 * @author mdurbin
 *
 */
public class DCTest {

    private DC dcSample1;

    @BeforeEach
    public void setUp() throws JAXBException {
        final InputStream dcInputStream = this.getClass().getClassLoader().getResourceAsStream("dc-sample1.xml");
        dcSample1 = DC.parseDC(dcInputStream);
    }

    @Test
    public void testBasicDCParsing() throws JAXBException, IllegalAccessException {
        Assertions.assertEquals("Title 2", dcSample1.title[1]);
        Assertions.assertEquals("Title 1", dcSample1.title[0]);
        Assertions.assertEquals("Creator 2", dcSample1.creator[1]);
    }

    @Test
    public void testHelperMethodContract() throws JAXBException, IllegalAccessException {
        final List<String> uris = dcSample1.getRepresentedElementURIs();
        for (final String uri : uris) {
            Assertions.assertFalse(dcSample1.getValuesForURI(uri).isEmpty());
        }
    }

    @Test
    public void testUnknownUriThrows() {
        Assertions.assertThrows(RuntimeException.class, () ->
            dcSample1.getValuesForURI(DC.DC_NS + "notADcElement"));
    }

    @Test
    public void testNullFieldReturnsNull() {
        Assertions.assertNull(new DC().getValuesForURI(DC.DC_NS + "title"));
    }

}
