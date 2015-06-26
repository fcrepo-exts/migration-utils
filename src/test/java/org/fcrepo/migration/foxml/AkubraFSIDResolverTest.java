package org.fcrepo.migration.foxml;

import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * @author mikedurbin
 */
public class AkubraFSIDResolverTest {

    private AkubraFSIDResolver idResolver;

    private File tempDir;

    @Before
    public void setup() throws IOException {
        tempDir = File.createTempFile("tempfile", "basedir");
        tempDir.delete();
        tempDir.mkdir();
        idResolver = new AkubraFSIDResolver(tempDir);
    }

    @Test
    public void testIDMapping() throws UnsupportedEncodingException {
        Assert.assertEquals("example:1+DS2+DS2.0",
                idResolver.getInternalIdForFile(new File("info%3Afedora%2Fexample%3A1%2FDS2%2FDS2.0")));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testBadFileIDMapping() throws UnsupportedEncodingException {
        idResolver.getInternalIdForFile(new File("example%3A1%2FDS2%2FDS2.0"));
    }


    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(tempDir);
    }
}
