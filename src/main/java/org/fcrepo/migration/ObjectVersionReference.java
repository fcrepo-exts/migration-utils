/**
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
 * Represents a version of a Fedora 3 object.
 *
 * TODO: perhaps the audit trail should be parsed and exposed here
 * @author mdurbin
 */
public interface ObjectVersionReference {

    /**
     * @return the ObjectReference object that encapsulates everything about
     * the underlying Fedora 3 object.
     */
    public ObjectReference getObject();

    /**
     * @return all the basic object information.  This is unversioned information.
     */
    public ObjectInfo getObjectInfo();

    /**
     * @return all the object properties.  This is unversioned information.
     */
    public ObjectProperties getObjectProperties();

    /**
     * @return the lastModifiedDate proeperty for this version.  This is formatted as
     * all Fedora 3 dates are formatted.
     */
    public String getVersionDate();

    /**
     * Lists the current version of all datastreams changed from the pervious version
     * to this one.
     * @return a List containing a DatastreamVersion for each datastream that changed
     * from the last version to this one.
     */
    public List<DatastreamVersion> listChangedDatastreams();

    /**
     * @return true if this is the first version.
     */
    public boolean isLastVersion();

    /**
     * @return true if this is the last version.
     */
    public boolean isFirstVersion();

    /**
     * @return the version index (0 for first, 1 for second, etc.) in chronological
     * order from oldest to newest.
     */
    public int getVersionIndex();

    /**
     * @param dsId of datastream to be tested for change.
     *
     * @return true if datastream with the given DSID changed as part of the
     * update that contributed to this version.
     */
    public boolean wasDatastreamChanged(String dsId);
}
