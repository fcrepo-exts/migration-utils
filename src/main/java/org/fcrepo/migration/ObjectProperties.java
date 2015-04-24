package org.fcrepo.migration;

import java.util.List;

/**
 * An interface defining access to the object-level properties for
 * a fedora 3 object.
 * @author mdurbin
 */
public interface ObjectProperties {

    /**
     * Gets the properties for the object.
     */
    public List<? extends ObjectProperty> listProperties();

}
