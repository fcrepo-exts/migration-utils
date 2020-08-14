/*
 * Copyright 2019 DuraSpace, Inc.
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

import edu.wisc.library.ocfl.api.MutableOcflRepository;
import org.fcrepo.storage.ocfl.OcflObjectSession;
import org.fcrepo.storage.ocfl.OcflObjectSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for VanillaOcflObjectSessions
 *
 * @author pwinckles
 */
public class VanillaOcflObjectSessionFactory implements OcflObjectSessionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaOcflObjectSessionFactory.class);

    private final MutableOcflRepository ocflRepo;
    private final Path stagingRoot;
    private final String defaultVersionMessage;
    private final String defaultVersionUserName;
    private final String defaultVersionUserAddress;

    private final Map<String, OcflObjectSession> sessions;

    private boolean closed = false;

    /**
     * @param ocflRepo the OCFL client
     * @param stagingRoot the root staging directory
     * @param defaultVersionMessage OCFL version message
     * @param defaultVersionUserName OCFL version user
     * @param defaultVersionUserAddress OCFL version user address
     */
    public VanillaOcflObjectSessionFactory(final MutableOcflRepository ocflRepo,
                                           final Path stagingRoot,
                                           final String defaultVersionMessage,
                                           final String defaultVersionUserName,
                                           final String defaultVersionUserAddress) {
        this.ocflRepo = ocflRepo;
        this.stagingRoot = stagingRoot;
        this.defaultVersionMessage = defaultVersionMessage;
        this.defaultVersionUserName = defaultVersionUserName;
        this.defaultVersionUserAddress = defaultVersionUserAddress;
        this.sessions = new ConcurrentHashMap<>();
    }

    @Override
    public OcflObjectSession newSession(final String ocflObjectId) {
        enforceOpen();

        final var sessionId = UUID.randomUUID().toString();
        final var session = new VanillaOcflObjectSession(
                sessionId,
                ocflRepo,
                ocflObjectId,
                stagingRoot.resolve(sessionId),
                () -> sessions.remove(sessionId)
        );

        session.versionAuthor(defaultVersionUserName, defaultVersionUserAddress);
        session.versionMessage(defaultVersionMessage);

        sessions.put(sessionId, session);
        return session;
    }

    @Override
    public Optional<OcflObjectSession> existingSession(final String sessionId) {
        enforceOpen();
        return Optional.ofNullable(sessions.get(sessionId));
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            final var sessions = new ArrayList<>(this.sessions.values());
            sessions.forEach(session -> {
                try {
                    session.abort();
                } catch (RuntimeException e) {
                    LOG.warn("Failed to close session {} cleanly.", session.sessionId(), e);
                }
            });
            ocflRepo.close();
        }
    }

    private void enforceOpen() {
        if (closed) {
            throw new IllegalStateException("The session factory is closed!");
        }
    }

}
