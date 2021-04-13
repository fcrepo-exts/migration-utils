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

import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.stream.Stream;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.fcrepo.storage.ocfl.CommitType;
import org.fcrepo.storage.ocfl.OcflObjectSession;
import org.fcrepo.storage.ocfl.OcflVersionInfo;
import org.fcrepo.storage.ocfl.ResourceContent;
import org.fcrepo.storage.ocfl.ResourceHeaders;

/**
 * A wrapper similar to the FcrepoOcflObjectSessionWrapper to time operations
 *
 * @author mikejritter
 */
public class OcflObjectSessionWrapper implements OcflObjectSession {

    private final OcflObjectSession inner;

    private static final String METRIC_NAME = "fcrepo.storage.ocfl.object";
    private static final String OPERATION = "operation";
    private static final Timer writeTimer = Metrics.timer(METRIC_NAME, OPERATION, "write");
    private static final Timer writeHeadersTimer = Metrics.timer(METRIC_NAME, OPERATION, "writeHeaders");
    private static final Timer deleteContentTimer = Metrics.timer(METRIC_NAME, OPERATION, "deleteContent");
    private static final Timer deleteResourceTimer = Metrics.timer(METRIC_NAME, OPERATION, "deleteResource");
    private static final Timer readHeadersTimer = Metrics.timer(METRIC_NAME, OPERATION, "readHeaders");
    private static final Timer readContentTimer = Metrics.timer(METRIC_NAME, OPERATION, "readContent");
    private static final Timer listVersionsTimer = Metrics.timer(METRIC_NAME, OPERATION, "listVersions");
    private static final Timer containsResourceTimer = Metrics.timer(METRIC_NAME, OPERATION, "containsResource");
    private static final Timer commitTimer = Metrics.timer(METRIC_NAME, OPERATION, "commit");


    public OcflObjectSessionWrapper(final OcflObjectSession inner) {
        this.inner = inner;
    }

    @Override
    public String sessionId() {
        return inner.sessionId();
    }

    @Override
    public String ocflObjectId() {
        return inner.ocflObjectId();
    }

    @Override
    public ResourceHeaders writeResource(final ResourceHeaders headers, final InputStream content) {
        return writeTimer.record(() -> inner.writeResource(headers, content));
    }

    @Override
    public void writeHeaders(final ResourceHeaders headers) {
        writeHeadersTimer.record(() -> inner.writeHeaders(headers));
    }

    @Override
    public void deleteContentFile(final ResourceHeaders headers) {
        deleteContentTimer.record(() -> inner.deleteContentFile(headers));
    }

    @Override
    public void deleteResource(final String resourceId) {
        deleteResourceTimer.record(() -> inner.deleteResource(resourceId));
    }

    @Override
    public boolean containsResource(final String resourceId) {
        return containsResourceTimer.record(() -> inner.containsResource(resourceId));
    }

    @Override
    public ResourceHeaders readHeaders(final String resourceId) {
        return readHeadersTimer.record(() -> inner.readHeaders(resourceId));
    }

    @Override
    public ResourceHeaders readHeaders(final String resourceId, final String versionNumber) {
        return readHeadersTimer.record(() -> inner.readHeaders(resourceId, versionNumber));
    }

    @Override
    public ResourceContent readContent(final String resourceId) {
        return readContentTimer.record(() -> inner.readContent(resourceId));
    }

    @Override
    public ResourceContent readContent(final String resourceId, final String versionNumber) {
        return readContentTimer.record(() -> inner.readContent(resourceId, versionNumber));
    }

    @Override
    public List<OcflVersionInfo> listVersions(final String resourceId) {
        return listVersionsTimer.record(() -> inner.listVersions(resourceId));
    }

    @Override
    public Stream<ResourceHeaders> streamResourceHeaders() {
        return inner.streamResourceHeaders();
    }

    @Override
    public void versionCreationTimestamp(final OffsetDateTime timestamp) {
        inner.versionCreationTimestamp(timestamp);
    }

    @Override
    public void versionAuthor(final String name, final String address) {
        inner.versionAuthor(name, address);
    }

    @Override
    public void versionMessage(final String message) {
        inner.versionMessage(message);
    }

    @Override
    public void commitType(final CommitType commitType) {
        inner.commitType(commitType);
    }

    @Override
    public void commit() {
        commitTimer.record(inner::commit);
    }

    @Override
    public void abort() {
        inner.abort();
    }

    @Override
    public void rollback() {
        inner.rollback();
    }

    @Override
    public boolean isOpen() {
        return inner.isOpen();
    }

    @Override
    public void close() {
        inner.close();
    }
}
