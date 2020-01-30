/**
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

package org.fcrepo.migration.handlers.ocfl;

import org.fcrepo.migration.ContentDigest;
import org.fcrepo.migration.DatastreamInfo;
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.ObjectInfo;
import org.fcrepo.migration.ObjectProperties;
import org.fcrepo.migration.ObjectReference;
import org.fcrepo.migration.ObjectVersionReference;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the ArchiveGroupHandler
 *
 * @author awoods
 * @since 2020-01-29
 */
@RunWith(MockitoJUnitRunner.Strict.class)
public class ArchiveGroupHandlerTest {

    private ArchiveGroupHandler handler;

    @Mock private OcflDriver ocflDriver;
    @Mock private OcflSession ocflSession;

    @Captor private ArgumentCaptor<String> filenameCaptor;


    @Before
    public void setup() {
        when(ocflDriver.Open(Mockito.anyString())).thenReturn(ocflSession);
    }

    @Test
    public void testDatastreamExtensions() {
        final boolean addDatastreamExtensions = true;

        handler = new ArchiveGroupHandler(ocflDriver, addDatastreamExtensions);

        handler.processObjectVersions(singletonList(createObjectVersionReference()));

        verify(ocflSession, times(6)).put(filenameCaptor.capture(), any(InputStream.class));

        final List<String> filenames = filenameCaptor.getAllValues();

        // With three datastream versions, 'put' should be called six times
        Assert.assertEquals(6, filenames.size());

        // Three 'put' calls writing metadata
        Assert.assertTrue(listAsString(filenames), filenames.remove("dsid.nt"));
        Assert.assertTrue(listAsString(filenames), filenames.remove("dsid.nt"));
        Assert.assertTrue(listAsString(filenames), filenames.remove("dsid.nt"));

        // One call for each of the mimetypes: text, rdf, jpg
        Assert.assertTrue(listAsString(filenames), filenames.contains("dsid.txt"));
        Assert.assertTrue(listAsString(filenames), filenames.contains("dsid.rdf"));
        Assert.assertTrue(listAsString(filenames), filenames.contains("dsid.jpg"));

    }

    private String listAsString(final List<String> filenames) {
        final StringBuilder sb = new StringBuilder();
        filenames.forEach(f -> sb.append(f).append(", "));
        sb.delete(sb.length() - 2, sb.length());
        return sb.toString();
    }

    // Mock classes below -------------------------------------

    @Mock private ObjectInfo objectInfo0;
    private ObjectVersionReference createObjectVersionReference() {
        return new ObjectVersionReference() {
            @Override
            public ObjectReference getObject() {
                return null;
            }

            @Override
            public ObjectInfo getObjectInfo() {
                when(objectInfo0.getPid()).thenReturn("pid");
                return objectInfo0;
            }

            @Override
            public ObjectProperties getObjectProperties() {
                return null;
            }

            @Override
            public String getVersionDate() {
                return null;
            }

            @Override
            public List<DatastreamVersion> listChangedDatastreams() {
                return asList(createDatastreamVersion("text/plain"),
                        createDatastreamVersion("application/rdf+xml"),
                        createDatastreamVersion("image/jpeg"));
            }

            @Override
            public boolean isLastVersion() {
                return false;
            }

            @Override
            public boolean isFirstVersion() {
                return false;
            }

            @Override
            public int getVersionIndex() {
                return 0;
            }

            @Override
            public boolean wasDatastreamChanged(final String dsId) {
                return false;
            }
        };
    }

    @Mock DatastreamInfo datastreamInfo;
    @Mock ObjectInfo objectInfo1;
    private DatastreamVersion createDatastreamVersion(final String mimeType) {

        return new DatastreamVersion() {
            @Override
            public DatastreamInfo getDatastreamInfo() {
                when(datastreamInfo.getControlGroup()).thenReturn("M");
                when(datastreamInfo.getObjectInfo()).thenReturn(objectInfo1);
                when(datastreamInfo.getDatastreamId()).thenReturn("dsid");
                when(objectInfo1.getPid()).thenReturn("pid");
                return datastreamInfo;
            }

            @Override
            public String getVersionId() {
                return null;
            }

            @Override
            public String getMimeType() {
                return mimeType;
            }

            @Override
            public String getLabel() {
                return null;
            }

            @Override
            public String getCreated() {
                return null;
            }

            @Override
            public String getAltIds() {
                return null;
            }

            @Override
            public String getFormatUri() {
                return null;
            }

            @Override
            public long getSize() {
                return 0;
            }

            @Override
            public ContentDigest getContentDigest() {
                return null;
            }

            @Override
            public InputStream getContent() throws IOException {
                return InputStream.nullInputStream();
            }

            @Override
            public String getExternalOrRedirectURL() {
                return "external-url";
            }

            @Override
            public boolean isFirstVersionIn(final ObjectReference obj) {
                return false;
            }

            @Override
            public boolean isLastVersionIn(final ObjectReference obj) {
                return false;
            }
        };
    }

}
