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
package org.fcrepo.migration.idmappers;

import org.fcrepo.migration.MigrationIDMapper;

/**
 * A simple MigrationIDMapper that converts pids to
 * paths using the following pattern:
 *
 * [namespace]/[01]/[23]/[45]/[56] where [namespace] is
 * the pid namespace, [01] is the first two characters in
 * the remainder of the pid, etc.
 *
 * This implementation ensures easy reversibility in mapping,
 * reasonable use of the hierarchy,
 *
 * This approach is deprecated due to issues described here:
 * https://jira.duraspace.org/browse/FCREPO-1547
 *
 * @author mdurbin
 */
@Deprecated
public class SimpleIDMapper implements MigrationIDMapper {

    private String baseUrl;

    private String rootPath;

    private int charDepth;

    /**
     * simple ID mapper.
     * @param baseUrl the base url for the server
     * @param rootPath the root path
     */
    public SimpleIDMapper(final String baseUrl, final String rootPath) {
        this.baseUrl = baseUrl;
        this.rootPath = rootPath;
        charDepth = 2;
    }

    /**
     * Sets the number of characters to use per level when converting
     * a pid to a path.  If it is known that all pids will be numeric,
     * a value or 3 would result in levels containing no more than 1000
     * children.
     * @param charDepth the number of characters to include in the
     *                  path segments created from the pid.
     */
    public void setCharDepth(final int charDepth) {
        if (charDepth < 1) {
            throw new IllegalArgumentException();
        }
        this.charDepth = charDepth;
    }

    /**
     * Gets the number of characters to use per level when converting
     * a pid to a path.
     * @return number of characters per level
     */
    public int getCharDepth() {
        return this.charDepth;
    }

    @Override
    public String mapObjectPath(final String pid) {
        return pidToPath(pid);
    }

    private String pidToPath(final String pid) {
        final StringBuffer path = new StringBuffer();
        path.append(rootPath);
        if (!rootPath.endsWith("/")) {
            path.append("/");
        }
        path.append(pid.substring(0, pid.indexOf(':')));
        for (int i = pid.indexOf(':') + 1; i < pid.length(); i += charDepth) {
            path.append('/');
            path.append(pid.substring(i, Math.min(i + charDepth, pid.length())));
        }
        return path.toString();
    }

    @Override
    public String mapDatastreamPath(final String pid, final String dsid) {
        return pidToPath(pid) + '/' + dsid;
    }

    @Override
    public String getBaseURL() {
        return this.baseUrl;
    }
}
