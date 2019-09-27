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

import java.io.InputStream;

import org.fcrepo.migration.Fedora4Client;

/**
 * Misuses Fedora4client to write OCFL objects.
 * <p>
 * Exploits knowledge of how OCFL-based f4 clients work internally in order to
 * write OCFL objects. It would be better to use an OCFL client directly.
 * </p>
 *
 * @author apb@jhu.edu
 */
public class HackyOcflDriver
implements OcflDriver {

    private final Fedora4Client client;

    /**
     * Create a HackyOCFLDriver.
     *
     * @param client
     *        Fedora client.
     */
    public HackyOcflDriver(final Fedora4Client client) {
        this.client = client;
    }

    @Override
    public OcflSession Open(final String id) {
        return new OcflSession() {

            @Override
            public void put(final String path, final InputStream content) {

                System.out.println("PUT content with path " + path);

                // We exploit knowledge that client chops up the given path into ocfl object id
                // path via the "/" separator. ContentType is unused by the OCFL Fedora4Client
                // impls.
                client.createOrUpdateNonRDFResource(id + "/" + path,
                                                    content,
                        "foo/dontCare");
            }

            @Override
            public void commit() {

                // versionID is unused by the OCFL Fedora4Client impls
                client.createVersionSnapshot(id, "foo");
            }
        };
    }

}
