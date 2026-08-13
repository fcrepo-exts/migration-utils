/*
 * Copyright 2021 DuraSpace, Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.fcrepo.storage.ocfl.CommitType;
import org.fcrepo.storage.ocfl.OcflObjectSession;
import org.fcrepo.storage.ocfl.OcflVersionInfo;
import org.fcrepo.storage.ocfl.ResourceContent;
import org.fcrepo.storage.ocfl.ResourceHeaders;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Verifies that {@link OcflObjectSessionWrapper} delegates every operation to the wrapped session.
 *
 * @author Dan Field
 */
@RunWith(MockitoJUnitRunner.class)
public class OcflObjectSessionWrapperTest {

    @Mock
    private OcflObjectSession inner;

    @Mock
    private ResourceHeaders headers;

    @Mock
    private ResourceContent content;

    private OcflObjectSessionWrapper wrapper;

    @Before
    public void setup() {
        wrapper = new OcflObjectSessionWrapper(inner);
    }

    @Test
    public void delegatesIdentifiers() {
        when(inner.sessionId()).thenReturn("session-1");
        when(inner.ocflObjectId()).thenReturn("info:fedora/obj");
        assertEquals("session-1", wrapper.sessionId());
        assertEquals("info:fedora/obj", wrapper.ocflObjectId());
    }

    @Test
    public void delegatesWriteResourceWithContent() {
        final InputStream content = new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8));
        when(inner.writeResource(org.mockito.ArgumentMatchers.eq(headers),
                org.mockito.ArgumentMatchers.any())).thenReturn(headers);
        assertEquals(headers, wrapper.writeResource(headers, content));
        verify(inner).writeResource(org.mockito.ArgumentMatchers.eq(headers),
                org.mockito.ArgumentMatchers.any(CountingInputStream.class));
    }

    @Test
    public void delegatesWriteResourceWithNullContent() {
        when(inner.writeResource(org.mockito.ArgumentMatchers.eq(headers),
                org.mockito.ArgumentMatchers.isNull())).thenReturn(headers);
        assertEquals(headers, wrapper.writeResource(headers, null));
        verify(inner).writeResource(headers, null);
    }

    @Test
    public void delegatesWriteHeaders() {
        wrapper.writeHeaders(headers);
        verify(inner).writeHeaders(headers);
    }

    @Test
    public void delegatesDeleteOperations() {
        wrapper.deleteContentFile(headers);
        verify(inner).deleteContentFile(headers);
        wrapper.deleteResource("res-1");
        verify(inner).deleteResource("res-1");
    }

    @Test
    public void delegatesContainsResource() {
        when(inner.containsResource("res-1")).thenReturn(true);
        assertEquals(true, wrapper.containsResource("res-1"));
    }

    @Test
    public void delegatesReadHeaders() {
        when(inner.readHeaders("res-1")).thenReturn(headers);
        when(inner.readHeaders("res-1", "v1")).thenReturn(headers);
        assertEquals(headers, wrapper.readHeaders("res-1"));
        assertEquals(headers, wrapper.readHeaders("res-1", "v1"));
    }

    @Test
    public void delegatesReadContentAndRanges() {
        when(inner.readContent("res-1")).thenReturn(content);
        when(inner.readContent("res-1", "v1")).thenReturn(content);
        when(inner.readRange("res-1", "v1", 0L, 10L)).thenReturn(content);
        when(inner.readRange("res-1", null, 0L, 10L)).thenReturn(content);

        assertEquals(content, wrapper.readContent("res-1"));
        assertEquals(content, wrapper.readContent("res-1", "v1"));
        assertEquals(content, wrapper.readRange("res-1", "v1", 0L, 10L));
        assertEquals(content, wrapper.readRange("res-1", 0L, 10L));
        verify(inner).readRange("res-1", null, 0L, 10L);
    }

    @Test
    public void delegatesListVersions() {
        final List<OcflVersionInfo> versions = Collections.emptyList();
        when(inner.listVersions("res-1")).thenReturn(versions);
        assertEquals(versions, wrapper.listVersions("res-1"));
    }

    @Test
    public void delegatesStreamResourceHeaders() {
        final Stream<ResourceHeaders> stream = Stream.of(headers);
        when(inner.streamResourceHeaders()).thenReturn(stream);
        assertEquals(stream, wrapper.streamResourceHeaders());
    }

    @Test
    public void delegatesVersioningMetadata() {
        final var timestamp = OffsetDateTime.now();
        wrapper.versionCreationTimestamp(timestamp);
        verify(inner).versionCreationTimestamp(timestamp);
        wrapper.versionAuthor("name", "address");
        verify(inner).versionAuthor("name", "address");
        wrapper.versionMessage("message");
        verify(inner).versionMessage("message");
        wrapper.invalidateCache("obj-1");
        verify(inner).invalidateCache("obj-1");
        wrapper.commitType(CommitType.NEW_VERSION);
        verify(inner).commitType(CommitType.NEW_VERSION);
    }

    @Test
    public void delegatesLifecycle() {
        when(inner.isOpen()).thenReturn(true);
        wrapper.commit();
        verify(inner).commit();
        wrapper.abort();
        verify(inner).abort();
        wrapper.rollback();
        verify(inner).rollback();
        assertEquals(true, wrapper.isOpen());
        wrapper.close();
        verify(inner).close();
    }

    @Test
    public void isOpenReflectsInner() {
        when(inner.isOpen()).thenReturn(false);
        assertFalse(wrapper.isOpen());
    }
}
