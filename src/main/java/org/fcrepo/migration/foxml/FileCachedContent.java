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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A CashedContent implementation that exposes content stored in a
 * file.
 * @author mdurbin
 */
public class FileCachedContent implements CachedContent {

    private File file;

    /**
     * File cached content
     * @param file the file
     */
    public FileCachedContent(final File file) {
        this.file = file;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        if (!file.exists()) {
            throw new IllegalStateException("Cached content is not available.");
        }
        return new FileInputStream(file);
    }
}
