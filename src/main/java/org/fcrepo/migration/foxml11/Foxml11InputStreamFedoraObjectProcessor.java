package org.fcrepo.migration.foxml11;

import org.fcrepo.migration.ContentDigest;
import org.fcrepo.migration.DatastreamInfo;
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.DefaultContentDigest;
import org.fcrepo.migration.DefaultObjectInfo;
import org.fcrepo.migration.StreamingFedoraObjectHandler;
import org.fcrepo.migration.FedoraObjectProcessor;
import org.fcrepo.migration.ObjectInfo;
import org.fcrepo.migration.ObjectProperties;

import org.apache.commons.codec.binary.Base64OutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A FedoraObjectProcessor implementation that uses the STaX API to process
 * a FOXML1.1 XML InputStream.
 */
public class Foxml11InputStreamFedoraObjectProcessor implements FedoraObjectProcessor {

    private static final String FOXML_11 = "info:fedora/fedora-system:def/foxml#";

    private URLFetcher fetcher;

    private InternalIDResolver idResolver;

    private XMLStreamReader reader;

    private List<File> tempFiles;

    /**
     * The basic object information read from the XML stream at construction
     * time by processing the root XML element and its attributes.
     */
    private ObjectInfo objectInfo;

    public Foxml11InputStreamFedoraObjectProcessor(InputStream is, URLFetcher fetcher, InternalIDResolver resolver) throws XMLStreamException {
        this.fetcher = fetcher;
        this.idResolver = resolver;
        final XMLInputFactory factory = XMLInputFactory.newFactory();
        reader = factory.createXMLStreamReader(is);
        reader.nextTag();
        Map<String, String> attributes = getAttributes(reader, "PID", "VERSION", "FEDORA_URI", "schemaLocation");
        if (!attributes.get("VERSION").equals("1.1")) {
            throw new RuntimeException("Only FOXML1.1 is currently supported.");
        }
        objectInfo = new DefaultObjectInfo(attributes.get("PID"), attributes.get("FEDORA_URI"));
        while (reader.next() == XMLStreamConstants.CHARACTERS) {
        }

        tempFiles = new ArrayList<File>();
    }

    @Override
    public ObjectInfo getObjectInfo() {
        return objectInfo;
    }

    @Override
    public void processObject(StreamingFedoraObjectHandler handler) {
        handler.beginObject(objectInfo);
        Foxml11DatastreamInfo dsInfo = null;
        try {
            handler.processObjectProperties(readProperties());
            while (reader.hasNext()) {
                if (reader.isCharacters()) {
                    if (!reader.isWhiteSpace()) {
                        throw new RuntimeException("Unexpected character data! \"" + reader.getText() + "\"");
                    } else {
                        // skip whitespace...
                    }
                } else if (reader.isStartElement()) {
                    if (reader.getLocalName().equals("datastream")
                            && reader.getNamespaceURI().equals(FOXML_11)) {
                        dsInfo = new Foxml11DatastreamInfo(objectInfo, reader);
                    } else if (reader.getLocalName().equals("datastreamVersion")) {
                        DatastreamVersion v = new Foxml11DatastreamVersion(dsInfo, reader);
                        handler.processDatastreamVersion(v);
                    } else {
                        throw new RuntimeException("Unexpected element! \"" + reader.getLocalName() + "\"!");
                    }
                } else if (reader.isEndElement() && (dsInfo != null && reader.getLocalName().equals("datastream"))) {
                    dsInfo = null;
                } else if (reader.isEndElement() && reader.getLocalName().equals("digitalObject")) {
                    // end of document....
                    handler.completeObject(objectInfo);
                    cleanUpTempFiles();
                } else {
                    throw new RuntimeException("Unexpected xml structure! \"" + reader.getEventType() + "\" at line "
                            + reader.getLocation().getLineNumber() + ", column " + reader.getLocation().getColumnNumber()
                            + "!" + (reader.isCharacters() ? "  \"" + reader.getText() + "\"" : ""));
                }
                reader.next();
            }

        } catch (XMLStreamException | JAXBException e) {
            handler.abortObject(objectInfo);
            cleanUpTempFiles();
            throw new RuntimeException(e);
        } finally {
            try {
                reader.close();
            } catch (XMLStreamException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void cleanUpTempFiles() {
        for (File f : this.tempFiles) {
            f.delete();
        }
    }

    private ObjectProperties readProperties() throws JAXBException, XMLStreamException {
        JAXBContext jc = JAXBContext.newInstance(Foxml11ObjectProperties.class);
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        JAXBElement<Foxml11ObjectProperties> p = unmarshaller.unmarshal(reader, Foxml11ObjectProperties.class);
        return p.getValue();
    }

    private void readUntilClosed(String name, String namespace) throws XMLStreamException {
        while (reader.hasNext()) {
            if (reader.isEndElement() && reader.getLocalName().equals(name) && reader.getNamespaceURI().equals(namespace)) {
                return;
            } else {
                // skip all other stuff....
            }
            reader.next();
        }
    }

    private class Foxml11DatastreamInfo implements DatastreamInfo {

        private String id;

        private String controlGroup;

        private String fedoraUri;

        private String state;

        private boolean versionable;

        private ObjectInfo objectInfo;

        public Foxml11DatastreamInfo(ObjectInfo objectInfo, XMLStreamReader reader) {
            this.objectInfo = objectInfo;
            Map<String, String> attributes
                    = getAttributes(reader, "ID", "CONTROL_GROUP", "FEDORA_URI", "STATE", "VERSIONABLE");
            id = attributes.get("ID");
            controlGroup = attributes.get("CONTROL_GROUP");
            fedoraUri = attributes.get("FEDORA_URI");
            state = attributes.get("STATE");
            versionable = Boolean.valueOf(attributes.get("VERSIONABLE"));
        }

        @Override
        public ObjectInfo getObjectInfo() {
            return objectInfo;
        }

        @Override
        public String getDatastreamId() {
            return id;
        }

        @Override
        public String getControlGroup() {
            return controlGroup;
        }

        @Override
        public String getFedoraURI() {
            return fedoraUri;
        }

        @Override
        public String getState() {
            return state;
        }

        @Override
        public boolean getVersionable() {
            return versionable;
        }
    }

    public class Foxml11DatastreamVersion implements DatastreamVersion {

        private DatastreamInfo dsInfo;

        private String id;
        private String label;
        private String created;
        private String mimeType;
        private String altIds;
        private String formatUri;
        private long size;
        private ContentDigest contentDigest;
        private CachedContent dsContent;

        public Foxml11DatastreamVersion(DatastreamInfo dsInfo, XMLStreamReader reader) throws XMLStreamException {
            this.dsInfo = dsInfo;
            Map<String, String> dsAttributes = getAttributes(reader, "ID", "LABEL", "CREATED", "MIMETYPE", "ALT_IDS", "FORMAT_URI", "SIZE");
            id = dsAttributes.get("ID");
            label = dsAttributes.get("LABEL");
            created = dsAttributes.get("CREATED");
            mimeType = dsAttributes.get("MIMETYPE");
            altIds = dsAttributes.get("ALT_IDS");
            formatUri = dsAttributes.get("FORMAT_URI");
            size = dsAttributes.containsKey("SIZE") ? Long.parseLong(dsAttributes.get("SIZE")) : -1;
            reader.next();

            while (reader.hasNext()) {
                if (reader.isCharacters()) {
                    if (!reader.isWhiteSpace()) {
                        throw new RuntimeException("Unexpected character data! \"" + reader.getText() + "\"");
                    } else {
                        // skip whitespace...
                    }
                } else if (reader.isStartElement()) {
                    String localName = reader.getLocalName();
                    if (localName.equals("contentDigest")) {
                        Map<String, String> attributes = getAttributes(reader, "TYPE", "DIGEST");
                        this.contentDigest = new DefaultContentDigest(attributes.get("TYPE"), attributes.get("DIGEST"));
                    } else if (localName.equals("xmlContent")) {
                        // this XML fragment may not be valid out of context
                        // context, so write it out as a complete XML
                        // file...
                        reader.next();
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        XMLEventReader eventReader = XMLInputFactory.newFactory().createXMLEventReader(reader);
                        XMLEventWriter eventWriter = XMLOutputFactory.newFactory().createXMLEventWriter(baos);
                        while (eventReader.hasNext()) {
                            XMLEvent event = eventReader.nextEvent();
                            if (event.isEndElement()
                                    && event.asEndElement().getName().getLocalPart().equals("xmlContent")
                                    && event.asEndElement().getName().getNamespaceURI().equals(FOXML_11)) {
                                eventWriter.close();
                                break;
                            } else {
                                eventWriter.add(event);
                            }
                        }
                        try {
                            dsContent = new MemoryCachedContent(new String(baos.toByteArray(), "UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (localName.equals("contentLocation")) {
                        Map<String, String> attributes = getAttributes(reader, "REF", "TYPE");
                        if (attributes.get("TYPE").equals("INTERNAL_ID")) {
                            dsContent = idResolver.resolveInternalID(attributes.get("REF"));
                        } else {
                            try {
                                dsContent = new URLCachedContent(new URL(attributes.get("REF")), fetcher);
                            } catch (MalformedURLException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } else if (localName.equals("binaryContent")) {
                        try {
                            File f = File.createTempFile("decoded", "file");
                            tempFiles.add(f);
                            Base64OutputStream out = new Base64OutputStream(new FileOutputStream(f), false);
                            while (reader.next() == XMLStreamConstants.CHARACTERS) {
                                out.write(reader.getText().getBytes("UTF-8"));
                            }
                            out.flush();
                            out.close();
                            dsContent = new FileCachedContent(f);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        readUntilClosed("binaryContent", FOXML_11);
                    } else {
                        throw new RuntimeException("Unexpected element! \"" + reader.getLocalName() + "\"!");
                    }
                } else if (reader.isEndElement()) {
                    if (reader.getLocalName().equals("datastreamVersion")) {
                        return;
                    }
                } else {
                    throw new RuntimeException("Unexpected xml structure! \"" + reader.getEventType() + "\" at line "
                            + reader.getLocation().getLineNumber() + ", column " + reader.getLocation().getColumnNumber()
                            + "!" + (reader.isCharacters() ? "  \"" + reader.getText() + "\"" : ""));
                }
                reader.next();
            }

        }

        @Override
        public DatastreamInfo getDatastreamInfo() {
           return dsInfo;
        }

        @Override
        public String getVersionId() {
            return id;
        }

        @Override
        public String getMimeType() {
            return mimeType;
        }

        @Override
        public String getLabel() {
            return label;
        }

        @Override
        public String getCreated() {
            return created;
        }

        @Override
        public String getAltIds() {
            return altIds;
        }

        @Override
        public String getFormatUri() {
            return formatUri;
        }

        @Override
        public long getSize() {
            return size;
        }

        @Override
        public ContentDigest getContentDigest() {
            return contentDigest;
        }

        @Override
        public InputStream getContent() throws IOException {
            return dsContent.getInputStream();
        }

        @Override
        public String getExternalOrRedirectURL() {
            if (dsContent instanceof URLCachedContent) {
                return ((URLCachedContent) dsContent).getURL().toString();
            } else {
                throw new IllegalStateException();
            }
        }

    }

    private static Map<String, String> getAttributes(XMLStreamReader r, String ... allowedNames) {
        HashMap<String, String> result = new HashMap<String, String>();
        Set<String> allowed = new HashSet<String>(Arrays.asList(allowedNames));
        for (int i = 0; i < r.getAttributeCount(); i ++) {
            final String localName = r.getAttributeLocalName(i);
            final String value = r.getAttributeValue(i);
            if (allowed.contains(localName)) {
                result.put(localName, value);
            } else {
                System.err.println("Unexpected attribute: " + localName + " = \"" + value + "\"");
            }
        }
        return result;

    }

}
