package org.fcrepo.migration.foxml;

import javax.xml.bind.annotation.XmlAttribute;

import org.fcrepo.migration.ObjectProperty;

/**
 * An ObjectProperty implementation that is annotated to allow
 * JAXB unmarshalling from a FOXML XML file or stream.
 * @author mdurbin
 */
public class FoxmlObjectProperty implements ObjectProperty {

    @XmlAttribute(name = "NAME")
    String name;

    @XmlAttribute(name = "VALUE")
    String value;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getValue() {
        return value;
    }
}
