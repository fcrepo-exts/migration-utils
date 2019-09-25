/**
 *
 */
package org.fcrepo.migration.f4clients;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import java.io.File;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.UUID;

/**
 * @author Remigiusz Malessa
 * @since 4.4.1-SNAPSHOT
 */
public class OCFLFedora4ClientTest {

    private static OCFLFedora4Client client;
    private static String storage;
    private static String staging;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUp() throws Exception {

        final String storage = "src/test/resources/ocflStorage";
        final String staging = "src/test/resources/staging";
        client = new OCFLFedora4Client(storage, staging, OCFLFedora4Client.ObjectIdMapperType.FLAT);
    }


    /**
     * @author Remigiusz Malessa
     * @since 4.4.1-SNAPSHOT
     */
    @Test
    public void testExists() {

        // Sanity check -
        final String badId = "nonexistantobject";
        assertFalse("Object should not exist: " + badId, client.exists(badId));

        final String goodId = "o1";
        assertTrue("Object should exist: " + goodId, client.exists(goodId));
    }

    /**
     * @author Dan Field
     * @since 4.4.1-SNAPSHOT
     */
    @Test
    public void testCreatePlaceholder() throws java.io.IOException {

        // Use a UUID to avoid collisions with other files / tests
        final String newFileName = UUID.randomUUID().toString();

        // Check that the path is returned for a pre-existing path
        // We can only compare the basename of the file as the full path is user specific
        final File notAPlaceholder = new File(staging, newFileName);
        notAPlaceholder.createNewFile();
        assertTrue("file " + newFileName + " should exist", notAPlaceholder.exists());

        // delete previous file and try to create another placeholder with at the same path
        // this time with the OCFL client
        // Check that the delete works first before starting the test
        assertTrue(notAPlaceholder.delete());
        // create new placeholder and test path matches
        final File returnedPath = new File(client.createPlaceholder(newFileName));
        // check returned file really exists
        assertTrue("file " + newFileName + " should exist", returnedPath.exists());
        // check the path name matches what we requested
        assertEquals(newFileName, returnedPath.getName());
        // finally try to create an additional placeholder with the same name
        final File placeholderReturnedPath = new File(client.createPlaceholder(newFileName));
        // check that we get returned the path name of the previous placeholder
        assertEquals(newFileName, placeholderReturnedPath.getName());
    }
}
