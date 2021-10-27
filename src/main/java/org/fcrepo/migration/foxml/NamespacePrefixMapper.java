/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
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
