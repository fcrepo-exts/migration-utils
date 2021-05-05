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
package org.fcrepo.migration.foxml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * An interface representing content that is accessible as an InputStream.
 * @author mdurbin
 */
public interface CachedContent {

    /**
     * get input stream.
     * @return the input stream
     * @throws IOException IO exception
     */
    public InputStream getInputStream() throws IOException;

    /**
     * get the file backing the CachedContent if it exists
     * @return the file
     */
    default Optional<File> getFile() {
        return Optional.empty();
    }
}
