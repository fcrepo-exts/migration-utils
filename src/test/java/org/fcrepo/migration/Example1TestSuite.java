package org.fcrepo.migration;

import org.apache.commons.io.IOUtils;
import org.fcrepo.migration.foxml11.CachedContent;
import org.fcrepo.migration.foxml11.Foxml11InputStreamFedoraObjectProcessor;
import org.fcrepo.migration.foxml11.InternalIDResolver;
import org.fcrepo.migration.foxml11.URLFetcher;
import org.junit.Assert;
import org.junit.Test;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * An abstract base class that defines some dummy classes useful for
 * testing a Migrator instance and a suite of tests appropriate for
 * a single example object.  Subclasses may expose this example object
 * in different ways.
 */
public abstract class Example1TestSuite {

    protected abstract DummyHandler getResult();

    protected abstract DummyURLFetcher getFetcher();


    @Test
    public void testObjectInfoParsing() {
        Assert.assertEquals("example:1", getResult().objectInfo.getPid());
        Assert.assertNull(getResult().objectInfo.getFedoraURI());
    }

    @Test
    public void testPropertiesParsing() {
        List<ObjectProperty> propertyList = (List<ObjectProperty>) getResult().properties.listProperties();
        Assert.assertEquals(5, propertyList.size());
        Assert.assertEquals("info:fedora/fedora-system:def/model#state", propertyList.get(0).getName());
        Assert.assertEquals("Active", propertyList.get(0).getValue());
        Assert.assertEquals("info:fedora/fedora-system:def/model#label", propertyList.get(1).getName());
        Assert.assertEquals("This is an example object.", propertyList.get(1).getValue());
        Assert.assertEquals("info:fedora/fedora-system:def/model#ownerId", propertyList.get(2).getName());
        Assert.assertEquals("exampleOwner", propertyList.get(2).getValue());
        Assert.assertEquals("info:fedora/fedora-system:def/model#createdDate", propertyList.get(3).getName());
        Assert.assertEquals("2015-01-27T19:07:33.120Z", propertyList.get(3).getValue());
        Assert.assertEquals("info:fedora/fedora-system:def/view#lastModifiedDate", propertyList.get(4).getName());
        Assert.assertEquals("2015-01-27T20:26:16.998Z", propertyList.get(4).getValue());
    }

    @Test
    public void testDatastreamParsing() throws XMLStreamException, IOException {
        Assert.assertEquals(7, getResult().dsVersions.size());
    }

    @Test
    public void testAuditDatastreamParsing() {
        final DatastreamVersion audit0 = getResult().dsVersions.get(0);
        Assert.assertEquals("AUDIT", audit0.getDatastreamInfo().getDatastreamId());
        Assert.assertEquals("A", audit0.getDatastreamInfo().getState());
        Assert.assertEquals("X", audit0.getDatastreamInfo().getControlGroup());
        Assert.assertFalse(audit0.getDatastreamInfo().getVersionable());
        Assert.assertEquals("AUDIT.0", audit0.getVersionId());
        Assert.assertEquals("Audit Trail for this object", audit0.getLabel());
        Assert.assertEquals("2015-01-27T19:07:33.120Z", audit0.getCreated());
        Assert.assertEquals("text/xml", audit0.getMimeType());
        Assert.assertEquals("info:fedora/fedora-system:format/xml.fedora.audit", audit0.getFormatUri());
    }

    @Test
    public void testDCDatastreamParsing() throws IOException {
        final DatastreamVersion dc0 = getResult().dsVersions.get(1);
        Assert.assertEquals("DC", dc0.getDatastreamInfo().getDatastreamId());
        Assert.assertEquals("A", dc0.getDatastreamInfo().getState());
        Assert.assertEquals("X", dc0.getDatastreamInfo().getControlGroup());
        Assert.assertTrue(dc0.getDatastreamInfo().getVersionable());
        Assert.assertEquals("DC1.0", dc0.getVersionId());
        Assert.assertEquals("Dublin Core Record for this object", dc0.getLabel());
        Assert.assertEquals("2015-01-27T19:07:33.120Z", dc0.getCreated());
        Assert.assertEquals("text/xml", dc0.getMimeType());
        Assert.assertEquals("http://www.openarchives.org/OAI/2.0/oai_dc/", dc0.getFormatUri());
        // Lengths are inconsistently accurate on inline XML :(
        //Assert.assertEquals(dc0.getSize(), IOUtils.toString(dc0.getContent()).length());
        Assert.assertEquals("<oai_dc:dc xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd\">\n" +
                "  <dc:title>This is an example object.</dc:title>\n" +
                "  <dc:identifier>example:1</dc:identifier>\n" +
                "</oai_dc:dc>", IOUtils.toString(dc0.getContent()).trim());
    }

    @Test
    public void testDS1VersionedDatastreamParsing() throws IOException {
        final DatastreamVersion ds1 = getResult().dsVersions.get(2);
        Assert.assertEquals("DS1", ds1.getDatastreamInfo().getDatastreamId());
        Assert.assertEquals("A", ds1.getDatastreamInfo().getState());
        Assert.assertEquals("X", ds1.getDatastreamInfo().getControlGroup());
        Assert.assertTrue(ds1.getDatastreamInfo().getVersionable());
        Assert.assertEquals("DS1.0", ds1.getVersionId());
        Assert.assertEquals("Example inline XML datastream", ds1.getLabel());
        Assert.assertEquals("2015-01-27T19:08:43.701Z", ds1.getCreated());
        Assert.assertEquals("text/xml", ds1.getMimeType());
        Assert.assertEquals("alternate_id", ds1.getAltIds());
        Assert.assertEquals("format:uri", ds1.getFormatUri());
        Assert.assertEquals(34, ds1.getSize());
        Assert.assertEquals("<test>\n" +
                "  This is a test.\n" +
                "</test>", IOUtils.toString(ds1.getContent()).trim());

        final DatastreamVersion ds2 = getResult().dsVersions.get(3);
        Assert.assertEquals("DS1", ds2.getDatastreamInfo().getDatastreamId());
        Assert.assertEquals("A", ds2.getDatastreamInfo().getState());
        Assert.assertEquals("X", ds2.getDatastreamInfo().getControlGroup());
        Assert.assertTrue(ds2.getDatastreamInfo().getVersionable());
        Assert.assertEquals("DS1.1", ds2.getVersionId());
        Assert.assertEquals("Example inline XML datastream", ds2.getLabel());
        Assert.assertEquals("2015-01-27T19:20:40.678Z", ds2.getCreated());
        Assert.assertEquals("text/xml", ds2.getMimeType());
        Assert.assertEquals("alternate_id", ds2.getAltIds());
        Assert.assertEquals("format:uri", ds2.getFormatUri());
        Assert.assertEquals(50, ds2.getSize());
        Assert.assertNull(ds2.getContentDigest());
        Assert.assertEquals("<test>\n" +
                "  This is a test that was edited.\n" +
                "</test>", IOUtils.toString(ds2.getContent()).trim());
    }

    @Test
    public void testDS2BinaryParsing() throws IOException {
        final DatastreamVersion ds2 = getResult().dsVersions.get(4);
        Assert.assertEquals("DS2", ds2.getDatastreamInfo().getDatastreamId());
        Assert.assertEquals("A", ds2.getDatastreamInfo().getState());
        Assert.assertEquals("M", ds2.getDatastreamInfo().getControlGroup());
        Assert.assertFalse(ds2.getDatastreamInfo().getVersionable());
        Assert.assertEquals("DS2.0", ds2.getVersionId());
        Assert.assertEquals("Example Managed binary datastream", ds2.getLabel());
        Assert.assertEquals("2015-01-27T19:09:18.112Z", ds2.getCreated());
        Assert.assertEquals("image/jpeg", ds2.getMimeType());
        Assert.assertEquals(46168, ds2.getSize());
        Assert.assertEquals("MD5", ds2.getContentDigest().getType());
        Assert.assertEquals("d4f18b8b9c64466819ddaad46228fb9b", ds2.getContentDigest().getDigest());
        Assert.assertTrue("Managed Base64 encoded datastream must be preserved.", IOUtils.contentEquals(
                getClass().getClassLoader().getResourceAsStream("small-mountains.jpg"),
                new ByteArrayInputStream(getResult().cachedDsVersionBinaries.get(4))));
    }

    @Test
    public void testDS3RedirectParsing() throws IOException {
        final DatastreamVersion ds3 = getResult().dsVersions.get(5);
        Assert.assertEquals("DS3", ds3.getDatastreamInfo().getDatastreamId());
        Assert.assertEquals("A", ds3.getDatastreamInfo().getState());
        Assert.assertEquals("R", ds3.getDatastreamInfo().getControlGroup());
        Assert.assertTrue(ds3.getDatastreamInfo().getVersionable());
        Assert.assertEquals("DS3.0", ds3.getVersionId());
        Assert.assertEquals("Example Redirect \u007Fdatastream.", ds3.getLabel());
        Assert.assertEquals("2015-01-27T19:14:05.948Z", ds3.getCreated());
        Assert.assertEquals("text/html", ds3.getMimeType());
        Assert.assertEquals(-1, ds3.getSize());
        ds3.getContent().close();
        Assert.assertEquals("http://local.fedora.server/fedora/describe", getFetcher().getLastUrl().toExternalForm());
    }

    @Test
    public void testDS4ExternalParsing() throws IOException {
        final DatastreamVersion ds4 = getResult().dsVersions.get(6);
        Assert.assertEquals("DS4", ds4.getDatastreamInfo().getDatastreamId());
        Assert.assertEquals("A", ds4.getDatastreamInfo().getState());
        Assert.assertEquals("E", ds4.getDatastreamInfo().getControlGroup());
        Assert.assertTrue(ds4.getDatastreamInfo().getVersionable());
        Assert.assertEquals("DS4.0", ds4.getVersionId());
        Assert.assertEquals("Example External datastream.", ds4.getLabel());
        Assert.assertEquals("2015-01-27T19:14:38.999Z", ds4.getCreated());
        Assert.assertEquals("text/html", ds4.getMimeType());
        Assert.assertEquals(-1, ds4.getSize());
        ds4.getContent().close();
        Assert.assertEquals("http://local.fedora.server/fedora", getFetcher().getLastUrl().toExternalForm());
    }

    public static class SimpleObjectSource implements ObjectSource {

        private FedoraObjectProcessor p;

        public SimpleObjectSource(String path, URLFetcher f, InternalIDResolver resolver) throws XMLStreamException {
            p = new Foxml11InputStreamFedoraObjectProcessor(getClass().getClassLoader().getResourceAsStream(path),
                    f, resolver);
        }

        @Override
        public Iterator<FedoraObjectProcessor> iterator() {
            return Collections.singletonList(p).iterator();
        }
    }

    class DummyHandler implements StreamingFedoraObjectHandler {

        ObjectInfo objectInfo;
        ObjectProperties properties;
        List<DatastreamVersion> dsVersions = new ArrayList<DatastreamVersion>();
        List<byte[]> cachedDsVersionBinaries = new ArrayList<byte[]>();

        @Override
        public void beginObject(ObjectInfo object) {
            this.objectInfo = object;
        }

        @Override
        public void processObjectProperties(ObjectProperties properties) {
            this.properties = properties;
        }

        @Override
        public void processDatastreamVersion(DatastreamVersion dsVersion) {
            dsVersions.add(dsVersion);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                IOUtils.copy(dsVersion.getContent(), baos);
                cachedDsVersionBinaries.add(baos.toByteArray());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }


        }

        @Override
        public void completeObject(ObjectInfo object) {

        }

        @Override
        public void abortObject(ObjectInfo object) {

        }
    }

    class DummyURLFetcher implements URLFetcher {

        private URL lastUrl;

        public URL getLastUrl() {
            return lastUrl;
        }

        @Override
        public InputStream getContentAtUrl(URL url) throws IOException {
            lastUrl = url;
            return new ByteArrayInputStream("DummyURLFetcher".getBytes("UTF-8"));
        }
    }

    class DummyIDResolver implements InternalIDResolver {
        @Override
        public CachedContent resolveInternalID(String id) {
            return null;
        }
    }

}
