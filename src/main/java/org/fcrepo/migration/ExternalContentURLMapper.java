package org.fcrepo.migration;

/**
 * An interface defining a method to replace one URL (represented as a String) with another.
 * In the context of migrating objects from fedora 3 to fedora 4, there may be a need to
 * make programmatic updates to the URLs founds in External or Redirect datastreams.  This
 * interface is for that purpose.
 *
 * @author Mike Durbin
 */
public interface ExternalContentURLMapper {

    /**
     * @param url to be mapped
     *
     * @return the String containing a URL that should be used instead of the given String
     * for migrated external or redirect datastreams.
     */
    public String mapURL(String url);
}
