package org.fcrepo.migration;

/**
 * An interface defining access to a specific object level property
 * for a fedora 3 object.
 * @author mdurbin
 */
public interface ObjectProperty {

    /**
     * @return the property name.
     */
    public String getName();

    /**
     * @return the property value.
     */
    public String getValue();
}
