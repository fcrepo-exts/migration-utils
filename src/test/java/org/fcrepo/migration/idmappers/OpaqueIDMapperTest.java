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
package org.fcrepo.migration.idmappers;

import org.fcrepo.client.FedoraException;
import org.fcrepo.client.FedoraObject;
import org.fcrepo.migration.f4clients.DefaultFedora4Client;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

/**
 * @author Mike Durbin
 */
public class OpaqueIDMapperTest {

    @Mock
    private DefaultFedora4Client client;

    @Mock
    private FedoraObject object;

    private OpaqueIDMapper mapper;

    final static String path = "/mock/path";

    @Before
    public void setup() throws FedoraException, IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(client.createPlaceholder(null)).thenReturn(path + "/1",  path + "/2", path + "/3");

        mapper = new OpaqueIDMapper(null, client);
    }

    @Test
    public void testObjectMintingAndCaching() throws FedoraException {
        final String pid1 = "pid:1";
        final String pid2 = "pid:2";

        final String path1 = mapper.mapObjectPath(pid1);
        Mockito.verify(client, Mockito.times(1)).createPlaceholder(null);

        final String path2 = mapper.mapObjectPath(pid2);
        Mockito.verify(client, Mockito.times(2)).createPlaceholder(null);

        final String path3 = mapper.mapObjectPath(pid1);
        Mockito.verify(client, Mockito.times(2)).createPlaceholder(null);

        Assert.assertEquals("Cached path should be returned!", path1, path3);
    }

    @Test
    public void testDSMintingAndCaching() throws FedoraException {
        final String pid1 = "pid:1";
        final String pid2 = "pid:2";
        final String ds1 = "ds1";
        final String ds2 = "ds2";

        final String path1 = mapper.mapDatastreamPath(pid1, ds1);
        Mockito.verify(client, Mockito.times(1)).createPlaceholder(null);
        Assert.assertEquals(path + "/1/" + ds1, path1);

        final String path2 = mapper.mapDatastreamPath(pid1, ds2);
        Mockito.verify(client, Mockito.times(1)).createPlaceholder(null);
        Assert.assertEquals(path + "/1/" + ds2, path2);

        final String path3 = mapper.mapDatastreamPath(pid2, ds1);
        Mockito.verify(client, Mockito.times(2)).createPlaceholder(null);
        Assert.assertEquals(path + "/2/" + ds1, path3);
    }
}
