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
package org.fcrepo.migration;

/**
 * An interface for a class that processes Fedora 3 objects
 * as an iteration of versions.  The single method
 * {@link #processObjectVersions} would be invoked once for
 * an object providing the entire version history (ie. times
 * with identifiable changes) in the Fedora 3 object
 * starting from the creation and proceeding chronologically
 * @author mdurbin
 */
public interface FedoraObjectVersionHandler {

    /**
     * Invoked to process a version of a Fedora 3 object.  All the metadata
     * and content that changed from the previous version to the one
     * represented by the current call is conventiently made available.
     * @param versions an iterable of Objects each encapsulating everything
     *               about a single version of a Fedora 3 object.
     */
    public void processObjectVersions(Iterable<ObjectVersionReference> versions);
}
