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
public class LegacyFSIDResolverTest {

    private LegacyFSIDResolver idResolver;

    private File tempDir;

    @Before
    public void setup() throws IOException {
        tempDir = File.createTempFile("tempfile", "basedir");
        tempDir.delete();
        tempDir.mkdir();
        idResolver = new LegacyFSIDResolver(tempDir);
    }

    @Test
    public void testIDMapping() throws UnsupportedEncodingException {
        Assert.assertEquals("example:1+DS2+DS2.0", idResolver.getInternalIdForFile(new File("example_1+DS2+DS2.0")));
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(tempDir);
    }
}
