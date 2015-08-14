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
package org.fcrepo.migration.foxml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * A simple abstraction around the functionality of reading
 * content from a URL as an InputStream.  Use of this interface
 * allows for pluggable implementations and easier testing.
 * @author mdurbin
 */
public interface URLFetcher {

    /**
     * get content from a url.
     * @param url the url
     * @return the content
     * @throws IOException IO exception
     */
    public InputStream getContentAtUrl(URL url) throws IOException;
}
