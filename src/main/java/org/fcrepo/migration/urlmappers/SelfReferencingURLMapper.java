package org.fcrepo.migration.urlmappers;

import org.fcrepo.migration.ExternalContentURLMapper;
import org.fcrepo.migration.MigrationIDMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An ExternalContentURLMapper implementation that updates redirects that point to the
 * fedora repository in which they originated to the destination of that pointed-to resource
 * in the fedora 4 repository to which the content is being migrated.
 *
 * For example, if "http://localhost:8080/fedora/objects/object:1/datastreams/POLICY" was a
 * redirect datastream in fedora 3 that redirected to
 * "http://localhost:8080/fedora/objects/policy:1/datastreams/XACML/content", this class would
 * supply the URL for the content of the migrated XACML datastream on the migrated policy:1
 * object.
 *
 * @author Mike Durbin
 */
public class SelfReferencingURLMapper implements ExternalContentURLMapper {

    private static final String OLD_DS_CONTENT_URL_PATTERN = "http://{local-fedora-server}/fedora/get/([^/]+)/(.+)";
    private static final String NEW_DS_CONTENT_URL_PATTERN
            = "http://{local-fedora-server}/fedora/objects/([^/]+)/datastreams/([^/]+)/content";

    private List<Pattern> contentPatterns;

    /**
     * A pattern that is compared after the content patterns, and if it matches,
     * an exception is thrown.  This is implemented to allow an error to be thrown
     * if any unmatched URLs that reference the fedora 3 repository are found; a
     * case that generally indicates a configuration error in the migration scenario.
     */
    private Pattern invalidPattern;

    private MigrationIDMapper idMapper;

    /**
     * Basic constructor.
     * @param localFedoraServer the domain and port for the server that hosted the fedora objects in the format
     *                          "localhost:8080".
     * @param idMapper the MigrationIDMapper used for the current migration scenario
     */
    public SelfReferencingURLMapper(final String localFedoraServer, final MigrationIDMapper idMapper) {
        this.contentPatterns = new ArrayList<Pattern>();
        this.contentPatterns.add(parsePattern(OLD_DS_CONTENT_URL_PATTERN, localFedoraServer));
        this.contentPatterns.add(parsePattern(NEW_DS_CONTENT_URL_PATTERN, localFedoraServer));
        this.idMapper = idMapper;

        this.invalidPattern = parsePattern("http://{local-fedora-server}/fedora/.*", localFedoraServer);
    }

    private Pattern parsePattern(final String pattern, final String localFedoraServer) {
        return Pattern.compile(pattern.replace("{local-fedora-server}", localFedoraServer));
    }

    @Override
    public String mapURL(final String url) {
        for (Pattern p : contentPatterns) {
            final Matcher m = p.matcher(url);
            if (m.matches()) {
                return idMapper.getBaseURL() + idMapper.mapDatastreamPath(m.group(1), m.group(2));
            }
        }
        if (invalidPattern.matcher(url).matches()) {
            throw new IllegalArgumentException("Unhandled internal external fedora 3 URL. (" + url + ")");
        }
        return url;
    }
}
