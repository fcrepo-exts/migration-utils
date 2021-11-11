/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration;

import java.util.List;

/**
 * An interface defining access to the object-level properties for
 * a fedora 3 object.
 * @author mdurbin
 */
public interface ObjectProperties {

    /**
     * @return the properties for the object.
     */
    public List<? extends ObjectProperty> listProperties();

}
