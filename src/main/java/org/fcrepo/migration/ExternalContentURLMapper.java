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
 * An interface defining a method to replace one URL (represented as a String) with another.
 * In the context of migrating objects from fedora 3 to fedora 4, there may be a need to
 * make programmatic updates to the URLs founds in External or Redirect datastreams.  This
 * interface is for that purpose.
 *
 * @author Mike Durbin
 */
public interface ExternalContentURLMapper {

    /**
     * @param url to be mapped
     *
     * @return the String containing a URL that should be used instead of the given String
     * for migrated external or redirect datastreams.
     */
    public String mapURL(String url);
}
