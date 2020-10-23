/*
 * Copyright 2015 DuraSpace, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fcrepo.migration.foxml;

import org.apache.commons.codec.binary.Base64OutputStream;
import org.fcrepo.migration.ContentDigest;
import org.fcrepo.migration.DatastreamInfo;
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.DefaultContentDigest;
import org.fcrepo.migration.DefaultObjectInfo;
import org.fcrepo.migration.FedoraObjectProcessor;
import org.fcrepo.migration.ObjectInfo;
import org.fcrepo.migration.ObjectProperties;
import org.fcrepo.migration.ObjectReference;
import org.fcrepo.migration.StreamingFedoraObjectHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * a FOXML XML InputStream.
 * @author mdurbin
 */
public class FoxmlInputStreamFedoraObjectProcessor implements FedoraObjectProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(FoxmlInputStreamFedoraObjectProcessor.class);

    private static final String FOXML_NS = "info:fedora/fedora-system:def/foxml#";

    private URLFetcher fetcher;

    private String localFedoraServer;

    private InternalIDResolver idResolver;

    private InputStream stream;

    private XMLStreamReader reader;

    private List<File> tempFiles;

    boolean isFedora2 = false;

    /**
     * The basic object information read from the XML stream at construction
     * time by processing the root XML element and its attributes.
     */
    private ObjectInfo objectInfo;

    /**
     * foxml input stream fedora object processor.
     * @param is the input stream
     * @param fetcher the fetcher
     * @param resolver the resolver
     * @param localFedoraServer the host and port (formatted like "localhost:8080") of the fedora 3 server
     *                          from which the content exposed by the "is" parameter comes.
     * @throws XMLStreamException xml stream exception
     */
    public FoxmlInputStreamFedoraObjectProcessor(final InputStream is, final URLFetcher fetcher,
                                                 final InternalIDResolver resolver, final String localFedoraServer)
            throws XMLStreamException {
        this.fetcher = fetcher;
        this.idResolver = resolver;
        this.localFedoraServer = localFedoraServer;
        final XMLInputFactory factory = XMLInputFactory.newFactory();
        stream = is;
        reader = factory.createXMLStreamReader(is);
        reader.nextTag();
        final Map<String, String> attributes = getAttributes(reader, "PID", "VERSION", "FEDORA_URI", "schemaLocation");
        if (attributes.get("VERSION") == null || !attributes.get("VERSION").equals("1.1")) {
            isFedora2 = true;
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
    public void processObject(final StreamingFedoraObjectHandler handler) {
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
                            && reader.getNamespaceURI().equals(FOXML_NS)) {
                        dsInfo = new Foxml11DatastreamInfo(objectInfo, reader);
                    } else if (reader.getLocalName().equals("datastreamVersion")) {
                        final DatastreamVersion v = new Foxml11DatastreamVersion(dsInfo, reader);
                        handler.processDatastreamVersion(v);
                    } else if (reader.getLocalName().equals("disseminator") && isFedora2) {
                        readUntilClosed("disseminator", FOXML_NS);
                        handler.processDisseminator();
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
                            + reader.getLocation().getLineNumber() + ", column "
                            + reader.getLocation().getColumnNumber()
                            + "!" + (reader.isCharacters() ? "  \"" + reader.getText() + "\"" : ""));
                }
                reader.next();
            }

        } catch (Exception e) {
            handler.abortObject(objectInfo);
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        } finally {
            cleanUpTempFiles();
            close();
        }
    }

    /**
     * Close resources associated to the processor
     */
    public void close() {
        try {
            reader.close();
        } catch (final XMLStreamException e) {
            LOG.warn("Failed to close reader cleanly", e);
        }
        try {
            stream.close();
        } catch (IOException e) {
            LOG.warn("Failed to close file cleanly", e);
        }
    }

    private void cleanUpTempFiles() {
        for (final File f : this.tempFiles) {
            if (f.exists()) {
                f.delete();
            }
        }
    }

    private ObjectProperties readProperties() throws JAXBException, XMLStreamException {
        final JAXBContext jc = JAXBContext.newInstance(FoxmlObjectProperties.class);
        final Unmarshaller unmarshaller = jc.createUnmarshaller();
        final JAXBElement<FoxmlObjectProperties> p = unmarshaller.unmarshal(reader, FoxmlObjectProperties.class);
        final FoxmlObjectProperties properties = p.getValue();
        if (isFedora2) {
            // Fedora 2 uses the rdf:type property with a literal value to differentiate between
            // objects, behavior mechanism objects and behavior definition objects.  That literal
            // cannot be retained as an rdf type in fedora4, nor can we use the generic mapping
            // to map it, so we convert it to a dcterms:type right here.
            for (FoxmlObjectProperty prop : properties.properties) {
                if (prop.getName().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
                    prop.name = "http://purl.org/dc/terms/type";
                }
            }
        }
        return properties;
    }

    private void readUntilClosed(final String name, final String namespace) throws XMLStreamException {
        while (reader.hasNext()) {
            if (reader.isEndElement() && reader.getLocalName().equals(name)
                    && reader.getNamespaceURI().equals(namespace)) {
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

        public Foxml11DatastreamInfo(final ObjectInfo objectInfo, final XMLStreamReader reader) {
            this.objectInfo = objectInfo;
            final Map<String, String> attributes
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

        /**
         * foxml datastream version.
         * @param dsInfo the datastream information
         * @param reader the reader
         * @throws XMLStreamException xml stream exception
         */
        public Foxml11DatastreamVersion(final DatastreamInfo dsInfo,
                final XMLStreamReader reader) throws XMLStreamException {
            this.dsInfo = dsInfo;
            final Map<String, String> dsAttributes = getAttributes(reader, "ID", "LABEL",
                    "CREATED", "MIMETYPE", "ALT_IDS", "FORMAT_URI", "SIZE");
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
                    final String localName = reader.getLocalName();
                    if (localName.equals("contentDigest")) {
                        final Map<String, String> attributes = getAttributes(reader, "TYPE", "DIGEST");
                        this.contentDigest = new DefaultContentDigest(attributes.get("TYPE"), attributes.get("DIGEST"));
                    } else if (localName.equals("xmlContent")) {
                        // this XML fragment may not be valid out of context
                        // context, so write it out as a complete XML
                        // file...
                        reader.next();
                        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        final XMLEventReader eventReader = XMLInputFactory.newFactory().createXMLEventReader(reader);
                        final XMLEventWriter eventWriter = XMLOutputFactory.newFactory().createXMLEventWriter(baos);
                        while (eventReader.hasNext()) {
                            final XMLEvent event = eventReader.nextEvent();
                            if (event.isEndElement()
                                    && event.asEndElement().getName().getLocalPart().equals("xmlContent")
                                    && event.asEndElement().getName().getNamespaceURI().equals(FOXML_NS)) {
                                eventWriter.close();
                                break;
                            } else {
                                eventWriter.add(event);
                            }
                        }
                        try {
                            dsContent = new MemoryCachedContent(new String(baos.toByteArray(), "UTF-8"));
                        } catch (final UnsupportedEncodingException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (localName.equals("contentLocation")) {
                        final Map<String, String> attributes = getAttributes(reader, "REF", "TYPE");
                        if (attributes.get("TYPE").equals("INTERNAL_ID")) {
                            dsContent = idResolver.resolveInternalID(attributes.get("REF"));
                        } else {
                            try {
                                String ref = attributes.get("REF");
                                if (ref.contains("local.fedora.server")) {
                                    ref = ref.replace("local.fedora.server", localFedoraServer);
                                }
                                dsContent = new URLCachedContent(new URL(ref), fetcher);
                            } catch (final MalformedURLException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } else if (localName.equals("binaryContent")) {
                        try {
                            final File f = File.createTempFile("decoded", "file");
                            tempFiles.add(f);
                            final Base64OutputStream out = new Base64OutputStream(new FileOutputStream(f), false);
                            while (reader.next() == XMLStreamConstants.CHARACTERS) {
                                out.write(reader.getText().getBytes("UTF-8"));
                            }
                            out.flush();
                            out.close();
                            dsContent = new FileCachedContent(f);
                        } catch (final IOException e) {
                            throw new RuntimeException(e);
                        }
                        readUntilClosed("binaryContent", FOXML_NS);
                    } else {
                        throw new RuntimeException("Unexpected element! \"" + reader.getLocalName() + "\"!");
                    }
                } else if (reader.isEndElement()) {
                    if (reader.getLocalName().equals("datastreamVersion")) {
                        return;
                    }
                } else {
                    throw new RuntimeException("Unexpected xml structure! \"" + reader.getEventType() + "\" at line "
                            + reader.getLocation().getLineNumber() + ", column "
                            + reader.getLocation().getColumnNumber()
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

        @Override
        public boolean isFirstVersionIn(final ObjectReference obj) {
            final List<DatastreamVersion> datastreams =
                    obj.getDatastreamVersions(getDatastreamInfo().getDatastreamId());
            return datastreams.indexOf(this) == 0;
        }

        @Override
        public boolean isLastVersionIn(final ObjectReference obj) {
            final List<DatastreamVersion> datastreams =
                    obj.getDatastreamVersions(getDatastreamInfo().getDatastreamId());
            return datastreams.indexOf(this) == datastreams.size() - 1;
        }
    }

    private static Map<String, String> getAttributes(final XMLStreamReader r,
            final String ... allowedNames) {
        final HashMap<String, String> result = new HashMap<String, String>();
        final Set<String> allowed = new HashSet<String>(Arrays.asList(allowedNames));
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
