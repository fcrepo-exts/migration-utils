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

/**
 * An interface for a class that processes Fedora 3 objects.
 * The single method {@link #processObject} would be invoked
 * for each object to be processed.
 * @author mdurbin
 */
public interface FedoraObjectHandler {

    /**
     * Invoked to process an object.  All the metadata and content
     * are accessible during this invocation.
     * @param object an object encapsulating everything about a single
     *               Fedora 3 object.
     */
    public void processObject(ObjectReference object);

}
