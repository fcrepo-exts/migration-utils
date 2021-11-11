/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
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
