package org.fcrepo.migration.foxml11;

import org.fcrepo.migration.ObjectProperties;
import org.fcrepo.migration.ObjectProperty;

import javax.xml.bind.annotation.XmlElement;
import java.util.Arrays;
import java.util.List;

/**
 * An ObjectProperties implementation that is annotated to allow
 * JAXB unmarshalling from a FOXML 1.1 XML file for stream.
 */
public class Foxml11ObjectProperties implements ObjectProperties {

    @XmlElement(name="property", namespace="info:fedora/fedora-system:def/foxml#")
    Foxml11ObjectProperty[] properties;

    @Override
    public List<? extends ObjectProperty> listProperties() {
        return Arrays.asList(properties);
    }
}
