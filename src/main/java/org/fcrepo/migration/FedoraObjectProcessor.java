package org.fcrepo.migration;

import javax.xml.stream.XMLStreamException;

/**
 * A class that encapsulates an object for processing.  This class represents a single object and
 * exposes methods to query basic information about it and then to process it with an arbitrary
 * StreamingFedoraObjectHandler.
 */
public interface FedoraObjectProcessor {

    public ObjectInfo getObjectInfo();

    public void processObject(StreamingFedoraObjectHandler handler) throws XMLStreamException;
}
