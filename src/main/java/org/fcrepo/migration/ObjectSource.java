package org.fcrepo.migration;

import java.util.Iterator;

/**
 * An interface representing a source of fedora 3 objects that can be
 * accessed sequentially for processing.
 */
public interface ObjectSource extends Iterable<FedoraObjectProcessor> {

}
