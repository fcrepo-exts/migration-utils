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

import java.util.Arrays;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;

import org.fcrepo.migration.ObjectProperties;
import org.fcrepo.migration.ObjectProperty;

/**
 * An ObjectProperties implementation that is annotated to allow
 * JAXB unmarshalling from a FOXML XML file for stream.
 * @author mdurbin
 */
public class FoxmlObjectProperties implements ObjectProperties {

    @XmlElement(name = "property", namespace = "info:fedora/fedora-system:def/foxml#")
    FoxmlObjectProperty[] properties;

    @Override
    public List<? extends ObjectProperty> listProperties() {
        return Arrays.asList(properties);
    }
}
