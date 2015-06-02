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
 * Utility bean to set 
 * @author danny
 *
 */
public class NamespacePrefixMapper {
    
    Properties namespacePrefixes;
    
    public NamespacePrefixMapper(File namespaceFile) throws IOException {
        namespacePrefixes = new Properties();
        FileInputStream namespaceInputStream = new FileInputStream(namespaceFile);
        namespacePrefixes.load(namespaceInputStream);
        namespaceInputStream.close();
    }
    
    public void setPrefixes(UpdateRequest updateRequest) {
        namespacePrefixes.forEach((prefix,namespace) -> updateRequest.setPrefix((String) prefix, (String) namespace));
    }
}
