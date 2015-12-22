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
package org.fcrepo.migration.urlmappers;

import junit.framework.Assert;
import org.fcrepo.migration.idmappers.SimpleIDMapper;
import org.junit.Test;

/**
 * @author Mike Durbin
 */
public class SelfReferencingURLMapperTest {

    private static final String FEDORA_4_BASE_URL = "info:fedora4/root";

    private static final String FEDORA_4_MIGRATION_ROOT = "migrated";

    private static final String LOCAL_FEDORA_SERVER = "placeholder:1234";

    private SimpleIDMapper idMapper;

    private SelfReferencingURLMapper urlMapper;

    public SelfReferencingURLMapperTest() {
        idMapper = new SimpleIDMapper(FEDORA_4_BASE_URL, FEDORA_4_MIGRATION_ROOT);
        urlMapper = new SelfReferencingURLMapper(LOCAL_FEDORA_SERVER, idMapper);
    }

    @Test
    public void testOldStyleContentURL() {
        final String pid = "test:1";
        final String dsId = "DS";
        final String dest = urlMapper.mapURL("http://" + LOCAL_FEDORA_SERVER + "/fedora/get/" + pid + "/" + dsId);
        Assert.assertEquals(idMapper.getBaseURL() + idMapper.mapDatastreamPath(pid, dsId), dest);
    }

    @Test
    public void testNewStyleContentURL() {
        final String pid = "test:1";
        final String dsId = "DS";
        final String dest = urlMapper.mapURL("http://" + LOCAL_FEDORA_SERVER + "/fedora/objects/" + pid
                + "/datastreams/" + dsId + "/content");
        Assert.assertEquals(idMapper.getBaseURL() + idMapper.mapDatastreamPath(pid, dsId), dest);
    }

    @Test
    public void testUnrelatedURL() {
        final String url = "http://localhost/non-fedora-url";
        final String mappedUrl = urlMapper.mapURL(url);
        Assert.assertTrue("Mapped URL is expected to equal unmapped url.", url.equals(mappedUrl));
        Assert.assertTrue("Mapped URL is expected to be same instance as unmapped url.", url == mappedUrl);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testNonContentFedora3URL() {
        urlMapper.mapURL("http://" + LOCAL_FEDORA_SERVER + "/fedora/example:1");
    }



}
