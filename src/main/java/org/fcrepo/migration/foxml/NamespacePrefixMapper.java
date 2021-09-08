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

import org.apache.jena.update.UpdateRequest;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility bean to set namespace prefixes in a SPARQL update.
 * @author danny
 *
 */
public class NamespacePrefixMapper {

    private final Properties namespacePrefixes;

    /**
     * Constructor.
     * @param namespaceFile Namespace properties file that gets injected in via Spring
     * @throws IOException Issues loading the properties file.
     */
    public NamespacePrefixMapper(final File namespaceFile) throws IOException {
        namespacePrefixes = new Properties();
        try (final InputStream namespaceInputStream = new BufferedInputStream(new FileInputStream(namespaceFile))) {
            namespacePrefixes.load(namespaceInputStream);
        }
    }

    /**
     * Declares all the namespace prefixes provided in the properties file.
     * @param updateRequest SPARQL update query that needs declared prefixes
     */
    public void setPrefixes(final UpdateRequest updateRequest) {
        namespacePrefixes.forEach((prefix,namespace) -> {
            updateRequest.setPrefix((String) prefix, (String) namespace);
        });
    }
}
