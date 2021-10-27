/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.foxml;

/**
 * @author mdurbin
 */
import java.io.File;
import java.util.regex.Pattern;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FoxmlDirectoryDFSIteratorTest {

    @Mock private File root;

    @Mock private File f1;

    @Before
    public void setup() {
        Mockito.when(root.listFiles()).thenReturn(new File[] { f1 });

        Mockito.when(f1.isFile()).thenReturn(true);
        Mockito.when(f1.getName()).thenReturn(".hidden");

    }

    @Test
    public void testNonHiddenInclusionPattern() {
        final FoxmlDirectoryDFSIterator i
               = new FoxmlDirectoryDFSIterator(root, null, null, new RegexFileFilter(Pattern.compile("^[^\\.].*$")));
        Assert.assertFalse("There must not be a matching file.", i.hasNext());
    }

    @Test
    public void testIncludeAllPattern() {
        final FoxmlDirectoryDFSIterator i
               = new FoxmlDirectoryDFSIterator(root, null, null, new RegexFileFilter(Pattern.compile(".*")));
        Assert.assertTrue("There should be a matching file.", i.hasNext());
    }

}
