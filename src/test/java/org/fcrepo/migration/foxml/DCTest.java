package org.fcrepo.migration.foxml;

import java.io.InputStream;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
/**
 *
 * @author mdurbin
 *
 */
public class DCTest {

    private DC dcSample1;

    @Before
    public void setUp() throws JAXBException {
        final InputStream dcInputStream = this.getClass().getClassLoader().getResourceAsStream("dc-sample1.xml");
        dcSample1 = DC.parseDC(dcInputStream);
    }

    @Test
    public void testBasicDCParsing() throws JAXBException, IllegalAccessException {
        Assert.assertEquals("Title 2", dcSample1.title[1]);
        Assert.assertEquals("Title 1", dcSample1.title[0]);
        Assert.assertEquals("Creator 2", dcSample1.creator[1]);
    }

    @Test
    public void testHelperMethodContract() throws JAXBException, IllegalAccessException {
        final List<String> uris = dcSample1.getRepresentedElementURIs();
        for (final String uri : uris) {
            Assert.assertFalse(dcSample1.getValuesForURI(uri).isEmpty());
        }
    }



}
