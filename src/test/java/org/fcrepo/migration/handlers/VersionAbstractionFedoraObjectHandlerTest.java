package org.fcrepo.migration.handlers;

import org.apache.commons.io.IOUtils;
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.Example1TestSuite;
import org.fcrepo.migration.FedoraObjectVersionHandler;
import org.fcrepo.migration.Migrator;
import org.fcrepo.migration.ObjectVersionReference;
import org.fcrepo.migration.foxml11.DirectoryScanningIDResolver;
import org.junit.Assert;
import org.junit.Test;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class VersionAbstractionFedoraObjectHandlerTest {

    @Test
    public void testObjectProcessing() throws IOException, XMLStreamException {
        TestingFedoraObjectVersionHandler vh = new TestingFedoraObjectVersionHandler();
        new Migrator(new Example1TestSuite.SimpleObjectSource("info%3Afedora%2Fexample%3A1", null,
                new DirectoryScanningIDResolver(new File("target/index"), 
                new File("src/test/resources/datastreamStore"))), 
                new ObjectAbstractionStreamingFedoraObjectHandler(new VersionAbstractionFedoraObjectHandler(vh))).run();
        Assert.assertEquals("Three versions should have been gleaned.", 6, vh.versions.size());
        Assert.assertEquals("2015-01-27T19:07:33.120Z", vh.versions.get(0).getVersionDate());
        Assert.assertEquals("AUDIT.0", vh.versions.get(0).listChangedDatastreams().get(0).getVersionId());
        Assert.assertEquals("DC1.0", vh.versions.get(0).listChangedDatastreams().get(1).getVersionId());

        Assert.assertEquals("2015-01-27T19:08:43.701Z", vh.versions.get(1).getVersionDate());
        Assert.assertEquals("DS1.0", vh.versions.get(1).listChangedDatastreams().get(0).getVersionId());

        Assert.assertEquals("2015-01-27T19:09:18.112Z", vh.versions.get(2).getVersionDate());
        Assert.assertEquals("DS2.0", vh.versions.get(2).listChangedDatastreams().get(0).getVersionId());

        Assert.assertEquals("2015-01-27T19:14:05.948Z", vh.versions.get(3).getVersionDate());
        Assert.assertEquals("DS3.0", vh.versions.get(3).listChangedDatastreams().get(0).getVersionId());

        Assert.assertEquals("2015-01-27T19:14:38.999Z", vh.versions.get(4).getVersionDate());
        Assert.assertEquals("DS4.0", vh.versions.get(4).listChangedDatastreams().get(0).getVersionId());

        Assert.assertEquals("2015-01-27T19:20:40.678Z", vh.versions.get(5).getVersionDate());
        Assert.assertEquals("DS1.1", vh.versions.get(5).listChangedDatastreams().get(0).getVersionId());
    }

    /**
     * An implementation of FedoraObjectVersionHandler which is meant to test the processing 
     * of a well-known fedora object. 
     */
    private static class TestingFedoraObjectVersionHandler implements FedoraObjectVersionHandler {

        List<ObjectVersionReference> versions;

        public TestingFedoraObjectVersionHandler() {
            versions = new ArrayList<ObjectVersionReference>();
        }

        /**
         * Tests that which is only testable within this method call and 
         * puts the reference on the versions list for later tests. 
         */
        @Override
        public void processObjectVersion(ObjectVersionReference reference) {
            versions.add(reference);
            for (DatastreamVersion dsv : reference.listChangedDatastreams()) {
                if (dsv.getDatastreamInfo().getControlGroup().equals("M")) {
                    try {
                        testDatastreamBinary(dsv);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        
        private void testDatastreamBinary(DatastreamVersion v) throws IOException {
            File temp = File.createTempFile("temporary", ".file");
            FileOutputStream fos = new FileOutputStream(temp);
            try {
                IOUtils.copy(v.getContent(), fos);
            } finally {
                fos.close();
            }
            Assert.assertEquals(v.getSize(), temp.length());
            temp.delete();
        }
    }
    
}
