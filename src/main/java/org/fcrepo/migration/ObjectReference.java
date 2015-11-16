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

import java.util.List;

/**
 * A reference to a Fedora 3 object.  Through the methods, all metadata and datastream
 * content is available.
 * @author mdurbin
 */
public interface ObjectReference {

    /**
     * @return all the basic object information.
     */
    public ObjectInfo getObjectInfo();

    /**
     * @return all the object properties.
     */
    public ObjectProperties getObjectProperties();

    /**
     * Lists all datastream ids.
     * @return all datastream ids
     */
    public List<String> listDatastreamIds();

    /**
     * Gets all versions of the datastream with the given id, from oldest to newest.
     * @param datastreamId the id (a value returned from listDatastreams()) of the
     *                     datastream whose versions are being requested.
     * @return a list of datastream versions ordered from oldest to newest.
     */
    public List<DatastreamVersion> getDatastreamVersions(String datastreamId);

    /**
     * @return true if the underlying object had a Fedora2-style disseminator
     *         that was lost in the migration.
     */
    public boolean hadFedora2Disseminators();
}
