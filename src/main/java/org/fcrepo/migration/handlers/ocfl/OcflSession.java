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

package org.fcrepo.migration.handlers.ocfl;

import java.io.InputStream;

/**
 * Stripped down OCFL session for writing to an object
 *
 * @author apb@jhu.edu
 **/
public interface OcflSession {

    /**
     * Add content to an object
     *
     * @param path
     *        Logical path (in OCFL terms)
     * @param content
     *        Bytes to be preserved
     */
    public void put(String path, InputStream content);

    /** Commit a version to OCFL. **/
    public void commit();
}
