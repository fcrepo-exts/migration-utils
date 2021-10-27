/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.foxml;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author awoods
 * @since 2020-01-27
 */
public class AkubraFSIDResolverIT {

    final static Logger LOGGER = getLogger(AkubraFSIDResolverIT.class);

    private static AkubraFSIDResolver resolver;

    @Test
    public void testWithEmptyIndexDir() throws IOException {

        final File testDir = new File("target/test/akubra");
        testDir.mkdirs();

        // Create empty dir
        final File indexDir = new File(testDir, "index");
        indexDir.mkdirs();

        final File dsRoot = new File("src/test/resources/akubraFS");

        resolver = new AkubraFSIDResolver(indexDir, dsRoot);

        final File[] files = indexDir.listFiles();
        Assert.assertTrue("There should be index files in the previously empty dir", files.length > 0);
    }

}
