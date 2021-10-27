/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration;

/**
 * An interface representing a source of fedora 3 objects that can be
 * accessed sequentially for processing.
 * @author mdurbin
 */
public interface ObjectSource extends Iterable<FedoraObjectProcessor> {

}
