package org.fcrepo.migration.handlers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.io.IOUtils;
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.FedoraObjectVersionHandler;
import org.fcrepo.migration.Migrator;
import org.fcrepo.migration.ObjectVersionReference;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class VersionAbstractionFedoraObjectHandlerTest {

    @Test
    public void testObjectProcessing() throws IOException, XMLStreamException {
        final ApplicationContext context = new ClassPathXmlApplicationContext("spring/version-abstraction.xml");
        final TestingFedoraObjectVersionHandler vh = (TestingFedoraObjectVersionHandler) context.getBean("versionHandler");
        final Migrator m = (Migrator) context.getBean("migrator");
        m.run();

        Assert.assertEquals("Six versions should have been gleaned.", 6, vh.versions.size());
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
        public void processObjectVersions(Iterable<ObjectVersionReference> versions) {
            for (ObjectVersionReference version : versions) {
                this.versions.add(version);
                for (final DatastreamVersion dsv : version.listChangedDatastreams()) {
                    if (dsv.getDatastreamInfo().getControlGroup().equals("M")) {
                        try {
                            testDatastreamBinary(dsv);
                        } catch (final IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }

        }

        private void testDatastreamBinary(final DatastreamVersion v) throws IOException {
            final File temp = File.createTempFile("temporary", ".file");
            final FileOutputStream fos = new FileOutputStream(temp);
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
