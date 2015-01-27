package org.fcrepo.migration;

/**
 * An interface defining access to a specific object level property
 * for a fedora 3 object.
 */
public interface ObjectProperty {

    /**
     * Gets the property name.
     */
    public String getName();

    /**
     * Gets the property value.
     */
    public String getValue();
}
