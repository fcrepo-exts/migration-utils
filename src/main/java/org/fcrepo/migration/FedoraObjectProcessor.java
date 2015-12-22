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
