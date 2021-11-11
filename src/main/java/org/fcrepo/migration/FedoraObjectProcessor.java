/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration;

import javax.xml.stream.XMLStreamException;

/**
 * A class that encapsulates an object for processing.  This class represents a single object and
 * exposes methods to query basic information about it and then to process it with an arbitrary
 * StreamingFedoraObjectHandler.
 * @author mdurbin
 */
public interface FedoraObjectProcessor extends AutoCloseable {

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

    /**
     * Close resources associated to the processor
     */
    void close();

}
