package org.fcrepo.migration;

/**
 * An interface representing a source of fedora 3 objects that can be
 * accessed sequentially for processing.
 * @author mdurbin
 */
public interface ObjectSource extends Iterable<FedoraObjectProcessor> {

}
