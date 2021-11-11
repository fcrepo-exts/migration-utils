/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration.pidlist;

/**
 * PidListManager implementations indicate that the Fedora Object associated with a PID
 *  should be processed, or not.
 *
 * @author awoods
 * @since 2019-11-08
 */
public interface PidListManager {

    /**
     * This method returns true if the the provided PID should be processed
     *
     * @param pid associated with a Fedora Object to potentially be processed
     * @return true if Object should be processed
     */
    boolean accept(String pid);


}
