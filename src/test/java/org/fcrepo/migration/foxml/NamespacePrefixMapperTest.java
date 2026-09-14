/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.migration.foxml;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.jena.update.UpdateRequest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Dan Field
 */
public class NamespacePrefixMapperTest {

    private File namespaceFile;

    @BeforeEach
    public void setup() throws IOException {
        namespaceFile = File.createTempFile("namespaces", ".properties");
        namespaceFile.deleteOnExit();
        Files.write(namespaceFile.toPath(),
                ("dc=http://purl.org/dc/elements/1.1/\n"
                        + "fedora=info:fedora/fedora-system:def/relations-external#\n")
                        .getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testSetPrefixesAppliesAllEntries() throws IOException {
        final NamespacePrefixMapper mapper = new NamespacePrefixMapper(namespaceFile);
        final UpdateRequest updateRequest = new UpdateRequest();

        mapper.setPrefixes(updateRequest);

        Assertions.assertEquals("http://purl.org/dc/elements/1.1/",
                updateRequest.getPrefixMapping().getNsPrefixURI("dc"));
        Assertions.assertEquals("info:fedora/fedora-system:def/relations-external#",
                updateRequest.getPrefixMapping().getNsPrefixURI("fedora"));
    }

    @Test
    public void testMissingFileThrows() {
        Assertions.assertThrows(IOException.class,
                () -> new NamespacePrefixMapper(new File("does-not-exist-12345.properties")));
    }
}
