/*
 * Licensed to DuraSpace under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * DuraSpace licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
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

/**
 * This class maps Fedora resources to locations on disk. It is based on this wiki:
 * https://wiki.lyrasis.org/display/FF/Design+-+Fedora+OCFL+Object+Structure
 *
 * This is a simplified implementation because F3 objects are always converted into flat AGs that only contain
 * binaries and binary descriptions.
 *
 * @author pwinckles
 * @since 6.0.0
 */
public final class PersistencePaths {

    private static final String HEADER_DIR = ".fcrepo/";
    private static final String ROOT_PREFIX = "fcr-root";
    private static final String CONTAINER_PREFIX = "fcr-container";
    private static final String DESCRIPTION_SUFFIX = "~fcr-desc";
    private static final String RDF_EXTENSION = ".nt";
    private static final String JSON_EXTENSION = ".json";

    private enum ResourceType {
        ROOT,
        BINARY,
        BINARY_DESCRIPTION;
    }

    private PersistencePaths() {
        // static class
    }

    /**
     * Constructs a path to a root header file.
     *
     * @param name the name of the resource
     * @return path to the resource's root header file
     */
    public static String rootHeaderPath(final String name) {
        return headerPath(ResourceType.ROOT, name);
    }

    /**
     * Constructs a path to a binary header file.
     *
     * @param name the name of the resource
     * @return path to a resource's binary header file
     */
    public static String binaryHeaderPath(final String name) {
        return headerPath(ResourceType.BINARY, name);
    }

    /**
     * Constructs a path to a binary description header file.
     *
     * @param name the name of the resource
     * @return path to a resource's binary description header file
     */
    public static String binaryDescHeaderPath(final String name) {
        return headerPath(ResourceType.BINARY_DESCRIPTION, name);
    }

    /**
     * Constructs a path to a root content file.
     *
     * @param name the name of the resource
     * @return path to the resource's root content file
     */
    public static String rootContentPath(final String name) {
        return contentPath(ResourceType.ROOT, name);
    }

    /**
     * Constructs a path to a binary content file.
     *
     * @param name the name of the resource
     * @return path to a resource's binary content file
     */
    public static String binaryContentPath(final String name) {
        return contentPath(ResourceType.BINARY, name);
    }

    /**
     * Constructs a path to a root content file.
     *
     * @param name the name of the resource
     * @return path to a resource's binary description content file
     */
    public static String binaryDescContentPath(final String name) {
        return contentPath(ResourceType.BINARY_DESCRIPTION, name);
    }

    private static String headerPath(final ResourceType type, final String name) {
        String path;

        if (type == ResourceType.ROOT) {
            path = ROOT_PREFIX;
        } else {
            path = name;
        }

        if (type == ResourceType.BINARY_DESCRIPTION) {
            path += DESCRIPTION_SUFFIX;
        }

        return headerPath(path);
    }

    private static String contentPath(final ResourceType type, final String name) {
        if (type == ResourceType.ROOT) {
            return CONTAINER_PREFIX + RDF_EXTENSION;
        }

        var path = name;

        if (type == ResourceType.BINARY_DESCRIPTION) {
            path += DESCRIPTION_SUFFIX + RDF_EXTENSION;
        }

        return path;
    }

    private static String headerPath(final String path) {
        return HEADER_DIR + path + JSON_EXTENSION;
    }

}
