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
 * An interface defining access to the high level identifying information
 * about a fedora 3 object.
 * @author mdurbin
 */
public interface ObjectInfo {

    /**
     * @return  the pid of the object.
     */
    public String getPid();

    /**
     * @return the Fedora URI of the object (or null if none is available in
     * the source).
     */
    public String getFedoraURI();

}
