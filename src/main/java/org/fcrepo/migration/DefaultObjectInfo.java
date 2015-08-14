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
 * A default implementation of ObjectInfo that accepts
 * values at construction time.
 * @author mdurbin
 */
public class DefaultObjectInfo implements ObjectInfo {

    private String pid;

    private String uri;

    /**
     * the default object info
     * @param pid the pid
     * @param uri the uri
     */
    public DefaultObjectInfo(final String pid, final String uri) {
        this.pid = pid;
        this.uri = uri;
    }

    @Override
    public String getPid() {
        return pid;
    }

    @Override
    public String getFedoraURI() {
        return uri;
    }

}
