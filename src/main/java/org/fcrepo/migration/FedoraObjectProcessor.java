package org.fcrepo.migration;

import javax.xml.stream.XMLStreamException;

/**
 * A class that encapsulates an object for processing.  This class represents a single object and
 * exposes methods to query basic information about it and then to process it with an arbitrary
 * StreamingFedoraObjectHandler.
 * @author mdurbin
 */
public interface FedoraObjectProcessor {

    /**
     * get object information.
     * @return the object info
     */
    public ObjectInfo getObjectInfo();

    /**
     * process the object.
     * @param handler the handler
     * @throws XMLStreamException xml stream exception
     */
    public void processObject(StreamingFedoraObjectHandler handler) throws XMLStreamException;
}
