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
