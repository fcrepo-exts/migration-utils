/*
 * Licensed to DuraSpace under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * DuraSpace licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fcrepo.migration.handlers.ocfl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author pwinckles
 */
public class PersistencePathsTest {

    private static final String FCREPO_DIR = ".fcrepo/";
    private static final String JSON = ".json";
    private static final String NT = ".nt";
    private static final String DESC_SUFFIX = "~fcr-desc";

    @Test
    public void mapRootToHeaderPath() {
        assertEquals(FCREPO_DIR + "fcr-root" + JSON,
                PersistencePaths.rootHeaderPath("blah"));
    }

    @Test
    public void mapRootToContentPath() {
        assertEquals("fcr-container" + NT,
                PersistencePaths.rootContentPath("blah"));
    }

    @Test
    public void mapBinaryToHeaderPath() {
        assertEquals(FCREPO_DIR + "blah" + JSON,
                PersistencePaths.binaryHeaderPath("blah"));
    }

    @Test
    public void mapBinaryToContentPath() {
        assertEquals("blah",
                PersistencePaths.binaryContentPath("blah"));
    }

    @Test
    public void mapBinaryDescToHeaderPath() {
        assertEquals(FCREPO_DIR + "blah" + DESC_SUFFIX + JSON,
                PersistencePaths.binaryDescHeaderPath("blah"));
    }

    @Test
    public void mapBinaryDescToContentPath() {
        assertEquals("blah" + DESC_SUFFIX + NT,
                PersistencePaths.binaryDescContentPath("blah"));
    }

}
