/**
 * 
 */
package org.fcrepo.migration.foxml11;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.hp.hpl.jena.update.UpdateRequest;

/**
 * Utility bean to set namespace prefixes in a SPARQL update.
 * @author danny
 *
 */
public class NamespacePrefixMapper {

    Properties namespacePrefixes;

    /**
     * Constructor.
     * @param namespaceFile Namespace properties file that gets injected in via Spring
     * @throws IOException
     */
    public NamespacePrefixMapper(final File namespaceFile) throws IOException {
        namespacePrefixes = new Properties();
        final FileInputStream namespaceInputStream = new FileInputStream(namespaceFile);
        namespacePrefixes.load(namespaceInputStream);
        namespaceInputStream.close();
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