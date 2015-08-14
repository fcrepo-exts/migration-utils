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

/**
 * An interface whose implementations represent methods to
 * convert Fedora 3 PIDs into fedora 4 paths.
 *
 * At one point it was thought that this should be more sophisticated
 * to support more advanced mapping (ie, passing more information about
 * the object, but in order to use this to resolve the "fedora:info/pid"
 * URI's within fedora's RELS-EXT we only have the pid.  Therefore
 * implementations that need to do something more sophisticated, should
 * build a mapping using whatever tooling it needs (and has available)
 * such that it can return the result with just the PID.
 *
 * @author mdurbin
 */
public interface MigrationIDMapper {

    /**
     * Takes a Fedora 3 pid and returns the path
     * that object would have in Fedora 4.
     * @param pid a PID for a Fedora 3 object.
     * @return a path suitable for use in Fedora 4.
     */
    public String mapObjectPath(String pid);

    /**
     * Takes a Fedora 3 PID and DSID and returns the path
     * that datastream would have in Fedora 4.
     * @param pid a PID for the Fedora 3 object
     * @param dsid the DS id for the Fedora 3 datastream
     * @return a path suitable for use in Fedora 4.
     */
    public String mapDatastreamPath(String pid, String dsid);

    /**
     * @return  the fedora 4 base URL.  Paths returned by
     * {@link #mapDatastreamPath} and {@link #mapObjectPath}
     * appended to this value will be resolvable URLs in the
     * fedora 4 repository.
     */
    public String getBaseURL();

}
