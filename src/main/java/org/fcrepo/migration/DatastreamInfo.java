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
 * An interface defining access to information about a fedora datastream.
 * @author mdurbin
 */
public interface DatastreamInfo {

    /**
     * Gets the information about the object to which this datastream
     * belongs.
     *
     * @return {@link org.fcrepo.migration.ObjectInfo}
     */
    public ObjectInfo getObjectInfo();

    /**
     * Gets the identifier for this datastream (Unique within an object).
     *
     * @return datastream id
     */
    public String getDatastreamId();

    /**
     * Gets the control group for this datastream.  This is expected to be
     * "M", "X", "R" or "E".
     *
     * @return control group
     */
    public String getControlGroup();

    /**
     * Gets the fedora URI for this datastream.
     *
     * @return Fedora URI
     */
    public String getFedoraURI();

    /**
     * Gets the state for this datastream.  This is expected to be "A", "I" or
     * "D".
     *
     * @return  state
     */
    public String getState();

    /**
     * Returns true if this datastream was/is versionable.
     *
     * @return true if versionable
     */
    public boolean getVersionable();
}
