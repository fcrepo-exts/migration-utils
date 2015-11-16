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

/**
 * An interface whose implementations serve as a mechanism to
 * resolve internal (to fedora/FOXML) IDs.
 * @author mdurdin
 */
public interface InternalIDResolver {

    /**
     * Gets the datastream for an internal ID.
     * @param id the internal id referenced within a FOXML file.
     * @return the binary content for the datastream referenced by the internal id
     */
    public CachedContent resolveInternalID(String id);
}
