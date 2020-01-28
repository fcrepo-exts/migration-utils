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
