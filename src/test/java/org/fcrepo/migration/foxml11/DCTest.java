package org.fcrepo.migration.foxml11;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.xml.bind.JAXBException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.List;

public class DCTest {
    
    private DC dcSample1;
    
    @Before
    public void setUp() throws JAXBException {
        InputStream dcInputStream = this.getClass().getClassLoader().getResourceAsStream("dc-sample1.xml");
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
        for (String uri : uris) {
            Assert.assertFalse(dcSample1.getValuesForURI(uri).isEmpty());
        }
    }



}
