package org.fcrepo.migration.handlers;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.modify.request.QuadAcc;
import com.hp.hpl.jena.sparql.modify.request.QuadDataAcc;
import com.hp.hpl.jena.sparql.modify.request.UpdateDataInsert;
import com.hp.hpl.jena.sparql.modify.request.UpdateDeleteWhere;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateRequest;
import org.apache.jena.atlas.io.IndentedWriter;
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.ExternalContentURLMapper;
import org.fcrepo.migration.Fedora4Client;
import org.fcrepo.migration.FedoraObjectVersionHandler;
import org.fcrepo.migration.MigrationIDMapper;
import org.fcrepo.migration.ObjectProperty;
import org.fcrepo.migration.ObjectReference;
import org.fcrepo.migration.ObjectVersionReference;
import org.fcrepo.migration.foxml11.DC;
import org.fcrepo.migration.foxml11.NamespacePrefixMapper;
import org.fcrepo.migration.urlmappers.SelfReferencingURLMapper;
import org.slf4j.Logger;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

/**
 *
 * @author mdurbin
 *
 */
public class BasicObjectVersionHandler implements FedoraObjectVersionHandler {

    private static Logger LOGGER = getLogger(BasicObjectVersionHandler.class);

    private static int suffix = 0;

    private Fedora4Client f4client;

    private MigrationIDMapper idMapper;

    private boolean importExternal;

    private boolean importRedirect;

    private ExternalContentURLMapper externalContentUrlMapper;

    private NamespacePrefixMapper namespacePrefixMapper;

    /**
     * Basic object version handler.
     * @param client a fedora4 client
     * @param idMapper the id mapper
     * @param localFedoraServer uri to fedora server
     */
    public BasicObjectVersionHandler(final Fedora4Client client,
                                     final MigrationIDMapper idMapper,
                                     final String localFedoraServer,
                                     final NamespacePrefixMapper namespacePrefixMapper) {
        this.f4client = client;
        this.idMapper = idMapper;
        this.externalContentUrlMapper = new SelfReferencingURLMapper(localFedoraServer, idMapper);
        this.namespacePrefixMapper = namespacePrefixMapper;
    }

    /**
     * A property setter for a property that determines the handling of External (X)
     * fedora 3 datastreams.  If true, the content of the URL to which those datastreams
     * redirect is fetched and ingested as a fedora 4-managed non-RDF resource.  If false
     * (default), a non-RDF resource is created in fedora 4 that when fetched results in
     * an HTTP redirect to the external url.
     *
     * @param value indicating if content is external
     */
    public void setImportExternal(final boolean value) {
        this.importExternal = value;
    }

    /**
     * A property setter for a property that determines the handling of Redirect (R)
     * fedora 3 datastreams.  If true, the content of the URL to which those datastreams
     * redirect is fetched and ingested as a fedora 4-managed non-RDF resource.  If false
     * (default), a non-RDF resource is created in fedora 4 that when fetched results in
     * an HTTP redirect to the external url.
     *
     * @param value indicating if content is imported
     */
    public void setImportRedirect(final boolean value) {
        this.importRedirect = value;
    }

    @Override
    public void processObjectVersions(final Iterable<ObjectVersionReference> versions) {
        String objectPath = null;
        try {
            for (final ObjectVersionReference version : versions) {

                LOGGER.debug("Considering object "
                        + version.getObjectInfo().getPid()
                        + " version at " + version.getVersionDate() + ".");

                if (objectPath == null) {
                    objectPath = idMapper.mapObjectPath(version.getObjectInfo().getPid());
                    if (!f4client.exists(objectPath)) {
                        f4client.createPlaceholder(objectPath);
                    } else if (!f4client.isPlaceholder(objectPath)) {
                        LOGGER.warn(objectPath + " already exists, skipping migration of "
                                + version.getObject().getObjectInfo().getPid() + "!");
                        return;
                    }
                }

                final QuadDataAcc triplesToInsert = new QuadDataAcc();
                final QuadAcc triplesToRemove = new QuadAcc();

                for (final DatastreamVersion v : withRELSINTLast(version.listChangedDatastreams())) {
                    LOGGER.debug("Considering changed datastream version " + v.getVersionId());
                    final String dsPath = idMapper.mapDatastreamPath(v.getDatastreamInfo().getObjectInfo().getPid(),
                            v.getDatastreamInfo().getDatastreamId());
                    if (v.getDatastreamInfo().getDatastreamId().equals("DC")) {
                        migrateDc(v, triplesToRemove, triplesToInsert);
                    } else if (v.getDatastreamInfo().getDatastreamId().equals("RELS-EXT")) {
                        migrateRelsExt(v, triplesToRemove, triplesToInsert);
                    } else if (v.getDatastreamInfo().getDatastreamId().equals("RELS-INT")) {
                        migrateRelsInt(v);
                    } else if ((v.getDatastreamInfo().getControlGroup().equals("E") && !importExternal)
                            || (v.getDatastreamInfo().getControlGroup().equals("R") && !importRedirect)) {
                        f4client.createOrUpdateRedirectNonRDFResource(dsPath,
                                externalContentUrlMapper.mapURL(v.getExternalOrRedirectURL()));
                    } else {
                        f4client.createOrUpdateNonRDFResource(dsPath, v.getContent(), v.getMimeType());
                    }
                }

                updateObjectProperties(version, objectPath, triplesToRemove, triplesToInsert);

                f4client.createVersionSnapshot(objectPath,
                        "imported-version-" + String.valueOf(version.getVersionIndex()));
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<DatastreamVersion> withRELSINTLast(final List<DatastreamVersion> orig) {
        final List<DatastreamVersion> versionsWithRELSINTLast = new ArrayList<DatastreamVersion>(orig);
        for (int i = 0; i < versionsWithRELSINTLast.size(); i ++) {
            if (versionsWithRELSINTLast.get(i).getDatastreamInfo().getDatastreamId().equals("RELS-INT")) {
                versionsWithRELSINTLast.add(versionsWithRELSINTLast.remove(i));
                break;
            }
        }
        return versionsWithRELSINTLast;
    }

    /**
     * Evaluates if an object/datastream property is a date.
     *
     * @param uri   The predicate in question.
     * @return      True if the property is a date.  False otherwise.
     */
    protected boolean isDateProperty(final String uri) {
        return uri.equals("info:fedora/fedora-system:def/model#createdDate") ||
                uri.equals("info:fedora/fedora-system:def/view#lastModifiedDate") ||
                uri.equals("http://www.loc.gov/premis/rdf/v1#hasDateCreatedByApplication") ||
                uri.equals("http://www.loc.gov/premis/rdf/v1#hasEventDateTime");
    }

    /**
     * Updates object properties after mapping them from 3 to 4.
     *
     * @param version           Object version to reference
     * @param objectPath        Destination path (in f4) for the object being migrated
     * @param triplesToRemove   List of triples to remove from resource.
     * @param triplesToInsert   List of triples to add to resource.

     */
    protected void updateObjectProperties(final ObjectVersionReference version,
            final String objectPath,
            final QuadAcc triplesToRemove,
            final QuadDataAcc triplesToInsert) {
        if (version.isFirstVersion()) {
            // Migration event (current time)
            final String now = getCurrentTimeInXSDDateTime();
            if (now != null) {
                addDateEvent(triplesToInsert,
                        "http://id.loc.gov/vocabulary/preservation/eventType/mig",
                        getCurrentTimeInXSDDateTime());
            }
        }

        if (version.isLastVersion()) {
            for (final ObjectProperty p : version.getObjectProperties().listProperties()) {
                mapProperty(p.getName(), p.getValue(), triplesToRemove, triplesToInsert, true);
            }
        }

        // Update if there's triples to remove / add.
        // Some may come from other datastreams like RELS-EXT and DC, not just
        // in this function.
        if (!triplesToInsert.getQuads().isEmpty() && !triplesToRemove.getQuads().isEmpty()) {
            updateResourceProperties(objectPath, triplesToRemove, triplesToInsert, false);
        }
    }

    /**
     * WIP function to map properties from 3 to 4.
     * Feel free to override this to suit your needs.
     *
     * @param origPred          Predicate of property to map from 3 to 4.
     * @param obj               Object of property to map from 3 to 4.
     * @param triplesToRemove   List of triples to remove from resource.
     * @param triplesToInsert   List of triples to add to resource.
     * @param isLiteral         TRUE if obj is a literal triple, FALSE if a URI
     */
    protected void mapProperty(final String origPred,
                               final String obj,
                               final QuadAcc triplesToRemove,
                               final QuadDataAcc triplesToInsert,
                               final Boolean isLiteral) {
        String pred = origPred;
        // Map dates and object state
        if (pred.equals("info:fedora/fedora-system:def/model#createdDate")) {
            pred = "http://www.loc.gov/premis/rdf/v1#hasDateCreatedByApplication";
        } else if (pred.equals("info:fedora/fedora-system:def/model#state")) {
            pred = "http://fedora.info/definitions/1/0/access/objState";
        } else if (pred.equals("info:fedora/fedora-system:def/view#lastModifiedDate")) {
            // Handle modified date seperately and exit early.
            addDateEvent(triplesToInsert,
                    "http://fedora.info/definitions/v4/audit#metadataModification",
                    obj);
            return;
        }

        if (isDateProperty(pred)) {
            updateDateTriple(triplesToRemove,
                             triplesToInsert,
                             pred,
                             obj);
        } else {
            if (isLiteral) {
                updateLiteralTriple(triplesToRemove,
                                    triplesToInsert,
                                    pred,
                                    obj);
            } else {
                updateUriTriple(triplesToRemove,
                               triplesToInsert,
                               pred,
                               obj);
            }
        }
    }

    /**
     * WIP utility function to update datastream properties.
     * Feel free to override this to suit your needs.
     *
     * @param obj    Object to operate upon
     * @param v      Version of the datasream to update.
     * @param dsPath resolved path (in f4) for the datastream
     */
    protected void updateDatastreamProperties(final ObjectReference obj,
            final DatastreamVersion v, final String dsPath) {
        final QuadDataAcc triplesToInsert = new QuadDataAcc();
        final QuadAcc triplesToRemove = new QuadAcc();

        final String createdDate = v.getCreated();

        // Get some initial properties
        if (v.isFirstVersionIn(obj)) {
            // Migration event (current time)
            final String now = getCurrentTimeInXSDDateTime();
            if (now != null) {
                addDateEvent(triplesToInsert,
                        "http://id.loc.gov/vocabulary/preservation/eventType/mig",
                        getCurrentTimeInXSDDateTime());
            }

            // Created date
            if (createdDate != null) {
                updateDateTriple(triplesToRemove,
                        triplesToInsert,
                        "http://www.loc.gov/premis/rdf/v1#hasDateCreatedByApplication",
                        createdDate);
            }
        }

        // Get the rest of the properties from the last version.
        if (v.isLastVersionIn(obj)) {
            // DSID
            final String dsid = v.getDatastreamInfo().getDatastreamId();
            if (dsid != null) {
                updateLiteralTriple(triplesToRemove,
                                    triplesToInsert,
                                    "http://purl.org/dc/terms/identifier",
                                    dsid);
            }

            // The created date of the last version is the last modified date.
            if (createdDate != null) {
                addDateEvent(triplesToInsert,
                        "http://fedora.info/definitions/v4/audit#contentModification",
                        createdDate);
            }

            // Label
            final String label = v.getLabel();
            if (label != null) {
                updateLiteralTriple(triplesToRemove,
                                    triplesToInsert,
                                    "http://purl.org/dc/terms/title",
                                    label);
            }

            // Object State
            final String state = v.getDatastreamInfo().getState();
            if (state != null) {
                updateLiteralTriple(triplesToRemove,
                                    triplesToInsert,
                                    "http://fedora.info/definitions/1/0/access/objState",
                                    state);
            }

            // Format URI
            final String formatUri = v.getFormatUri();
            if (formatUri != null) {
                updateLiteralTriple(triplesToRemove,
                                    triplesToInsert,
                                    "http://www.loc.gov/premis/rdf/v1#formatDesignation",
                                    formatUri);
            }
        }

        // Only do the update if you've got stuff to change.
        if (!triplesToInsert.getQuads().isEmpty() && !triplesToRemove.getQuads().isEmpty()) {
            updateResourceProperties(dsPath, triplesToRemove, triplesToInsert, true);
        }
    }

    /**
     * Migrates a RELS-EXT datastream by splitting it apart into triples to
     * update on the object it describes.
     *
     * @param v                 Version of the datasream to migrate.
     * @param triplesToRemove   List of triples to remove from resource.
     * @param triplesToInsert   List of triples to add to resource.
     *
     * @throws IOException on error
     */
    protected void migrateRelsExt(final DatastreamVersion v, final QuadAcc triplesToRemove,
                                  final QuadDataAcc triplesToInsert) throws IOException {
        // Get the identifier for the object this describes
        final String objectUri = "info:fedora/" + v.getDatastreamInfo().getObjectInfo().getPid();

        // Read the RDF
        final Model m = ModelFactory.createDefaultModel();
        m.read(v.getContent(), null);
        final StmtIterator statementIt = m.listStatements();
        while (statementIt.hasNext()) {
            final Statement s = statementIt.nextStatement();
            if (s.getSubject().getURI().equals(objectUri)) {
                final String predicateUri = s.getPredicate().getURI();
                if (s.getObject().isLiteral()) {
                    mapProperty(predicateUri,
                                s.getObject().asLiteral().getString(),
                                triplesToRemove,
                                triplesToInsert,
                                true);
                } else if (s.getObject().isURIResource()) {
                    mapProperty(predicateUri,
                                s.getObject().asResource().getURI(),
                                triplesToRemove,
                                triplesToInsert,
                                false);
                } else {
                    throw new RuntimeException("No current handling for non-URI," +
                            " non-Literal subjects in Fedora RELS-INT.");
                }
            } else {
                throw new RuntimeException("Non-resource subject found: " + s.getSubject().getURI());
            }
        }
    }

    /**
     * Migrates a RELS-INT datastream by splitting it apart and updating the
     * other datastreams it describes.
     *
     * @param v     Version of the datasream to migrate.
     *
     * @throws java.io.IOException on error
     * @throws java.lang.RuntimeException on error
     */
    protected void migrateRelsInt(final DatastreamVersion v) throws IOException, RuntimeException {
        // Read the RDF.
        final Model m = ModelFactory.createDefaultModel();
        m.read(v.getContent(), null);
        final StmtIterator statementIt = m.listStatements();
        while (statementIt.hasNext()) {
            // Get the datastream this triple describes.
            final Statement s = statementIt.nextStatement();
            final String dsUri = s.getSubject().getURI();
            final String[] splitUri = dsUri.split("/");
            final String dsId = splitUri[splitUri.length - 1];
            final String dsPath = idMapper.mapDatastreamPath(v.getDatastreamInfo().getObjectInfo().getPid(), dsId);
            if (!f4client.exists(dsPath)) {
                f4client.createDSPlaceholder(dsPath);
                LOGGER.warn("The datastream \"" + dsId
                        + "\" referenced in the RDF datastream \"" + v.getDatastreamInfo().getDatastreamId() + "\" on "
                        + v.getDatastreamInfo().getObjectInfo().getPid() + " did not exist at "
                        + v.getCreated() + ", making a placeholder!");
            }

            // Update this datastream with the RELS-INT RDF.
            final QuadDataAcc triplesToInsert = new QuadDataAcc();
            final QuadAcc triplesToRemove = new QuadAcc();

            final String pred = s.getPredicate().getURI();

            if (s.getObject().isLiteral()) {
                mapProperty(pred,
                            s.getObject().asLiteral().getString(),
                            triplesToRemove,
                            triplesToInsert,
                            true);
            } else if (s.getObject().isURIResource()) {
                mapProperty(pred,
                            s.getObject().asResource().getURI(),
                            triplesToRemove,
                            triplesToInsert,
                            false);
            } else {
                throw new RuntimeException("No current handling for non-URI, non-Literal subjects in Fedora RELS-INT.");
            }

            updateResourceProperties(dsPath, triplesToRemove, triplesToInsert, true);
        }
    }

    /**
     * Migrates a DC datastream by shredding it into RDF properties and
     * applying them directly to the object.
     *
     * @param v                 Version of the datasream to migrate.
     * @param triplesToRemove   List of triples to remove from resource.
     * @param triplesToInsert   List of triples to add to resource.
     *
     * @throws java.io.IOException on error
     * @throws java.lang.RuntimeException on error
     */
    protected void migrateDc(final DatastreamVersion v, final QuadAcc triplesToRemove,
                             final QuadDataAcc triplesToInsert) throws IOException, RuntimeException {
        try {
            final DC dc = DC.parseDC(v.getContent());
            for (String uri : dc.getRepresentedElementURIs()) {
                triplesToRemove.addTriple(new Triple(NodeFactory.createURI(""), NodeFactory.createURI(uri),
                        NodeFactory.createVariable("o")));
                for (String value : dc.getValuesForURI(uri)) {
                    triplesToInsert.addTriple(new Triple(NodeFactory.createURI(""), NodeFactory.createURI(uri),
                            NodeFactory.createLiteral(value)));
                    LOGGER.debug("Adding " + uri + " value " + value);
                }
            }
        } catch (JAXBException e) {
            throw new RuntimeException("Error parsing DC datastream " + v.getVersionId());
        }
    }

    /**
     * Utility function for updating a FedoraResource's properties.
     *
     * @param path              Path to the fedora resource to update.
     * @param triplesToRemove   List of triples to remove from resource.
     * @param triplesToInsert   List of triples to add to resource.
     * @param isNonRDF          true if the resource is a non-RDF resource.
     *
     * @throws RuntimeException Possible FedoraExcpetions and IOExceptions
     */
    protected void updateResourceProperties(final String path,
            final QuadAcc triplesToRemove,
            final QuadDataAcc triplesToInsert,
            final boolean isNonRDF) throws RuntimeException {
        try {
            final UpdateRequest updateRequest = UpdateFactory.create();
            namespacePrefixMapper.setPrefixes(updateRequest);
            updateRequest.add(new UpdateDeleteWhere(triplesToRemove));
            updateRequest.add(new UpdateDataInsert(triplesToInsert));
            final ByteArrayOutputStream sparqlUpdate = new ByteArrayOutputStream();
            updateRequest.output(new IndentedWriter(sparqlUpdate));
            LOGGER.trace("SPARQL: " + sparqlUpdate.toString("UTF-8"));
            if (isNonRDF) {
                f4client.updateNonRDFResourceProperties(path, sparqlUpdate.toString("UTF-8"));
            } else {
                f4client.updateResourceProperties(path, sparqlUpdate.toString("UTF-8"));
            }
            suffix = 0;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Utility function for updating a literal triple.
     *
     * @param triplesToRemove   List of triples to remove from resource.
     * @param triplesToInsert   List of triples to add to resource.
     * @param predicate         Predicate of relationship (assumed to be URI).
     * @param object            Object of relationship (assumed to be literal).
     */
    protected void updateLiteralTriple(final QuadAcc triplesToRemove,
                                       final QuadDataAcc triplesToInsert,
                                       final String predicate,
                                       final String object) {
        triplesToRemove.addTriple(new Triple(NodeFactory.createURI(""),
                NodeFactory.createURI(predicate),
                NodeFactory.createVariable("o" + String.valueOf(suffix))));
        triplesToInsert.addTriple(new Triple(NodeFactory.createURI(""),
                NodeFactory.createURI(predicate),
                NodeFactory.createLiteral(object)));
        suffix++;
    }

    /**
     * Utility function for updating a uri triple.
     *
     * @param triplesToRemove   List of triples to remove from resource.
     * @param triplesToInsert   List of triples to add to resource.
     * @param predicate         Predicate of relationship (assumed to be URI).
     * @param object            Object of relationship (assumed to URI).
     */
    protected void updateUriTriple(final QuadAcc triplesToRemove,
                                   final QuadDataAcc triplesToInsert,
                                   final String predicate,
                                   final String object) {
        final String newObjectUri = resolveInternalURI(object);
        triplesToRemove.addTriple(new Triple(NodeFactory.createURI(""),
                                             NodeFactory.createURI(predicate),
                                             NodeFactory.createVariable("o" + String.valueOf(suffix))));
        triplesToInsert.addTriple(new Triple(NodeFactory.createURI(""),
                                             NodeFactory.createURI(predicate),
                                             NodeFactory.createURI(newObjectUri)));
        suffix++;
    }

    /**
     * Takes a URI (String) and if it appears to be an internal Fedora URI ("info:fedora/pid")
     * the migrated URI for that resource is returned (and a placeholder is created in the
     * repository if it doesn't already exist).  Otherwise the value is returned unmodified.
     *
     * @param uri to be resolved
     *
     * @return string which is either the migrated URI or the unmodified URI
     */
    protected String resolveInternalURI(final String uri) {
        if (uri.startsWith("info:fedora/")) {
            final String path = idMapper.mapObjectPath(uri.substring("info:fedora/".length()));
            f4client.createPlaceholder(path);
            return f4client.getRepositoryUrl() + path;
        }
        return uri;
    }

    /**
     * Utility function for updating a date triple.
     *
     * @param triplesToRemove   List of triples to remove from resource.
     * @param triplesToInsert   List of triples to add to resource.
     * @param predicate         Predicate of relationship (assumed to be URI).
     * @param object            Object of relationship (assumed to be literal).
     */
    protected void updateDateTriple(final QuadAcc triplesToRemove,
            final QuadDataAcc triplesToInsert,
            final String predicate,
            final String object) {
        triplesToRemove.addTriple(new Triple(NodeFactory.createURI(""),
                NodeFactory.createURI(predicate),
                NodeFactory.createVariable("o" + String.valueOf(suffix))));
        triplesToInsert.addTriple(new Triple(NodeFactory.createURI(""),
                NodeFactory.createURI(predicate),
                NodeFactory.createLiteral(object, XSDDatatype.XSDdateTime)));
        suffix++;
    }

    /**
     * Utility function for adding a premis date event.  Current
     * implementation utilizes a blank node.
     *
     * @param triplesToInsert   List of triples to add to resource.
     * @param eventTypeURI      Type of premis event.
     * @param object            Object of relationship (e.g. the date.  Assumed to be literal).
     */
    protected void addDateEvent(final QuadDataAcc triplesToInsert,
            final String eventTypeURI,
            final String object) {
        final String eventPred = "http://www.loc.gov/premis/rdf/v1#hasEvent";
        final String eventTypePred = "http://www.loc.gov/premis/rdf/v1#hasEventType";
        final String eventDatePred = "http://www.loc.gov/premis/rdf/v1#hasEventDateTime";
        final Node bnode = NodeFactory.createAnon();

        triplesToInsert.addTriple(new Triple(NodeFactory.createURI(""),
                NodeFactory.createURI(eventPred),
                bnode));
        triplesToInsert.addTriple(new Triple(bnode,
                NodeFactory.createURI(eventTypePred),
                NodeFactory.createURI(eventTypeURI)));
        triplesToInsert.addTriple(new Triple(bnode,
                NodeFactory.createURI(eventDatePred),
                NodeFactory.createLiteral(object, XSDDatatype.XSDdateTime)));
    }

    /**
     * Utility function to get the current time properly formatted for SPARQL
     * or XML.
     *
     * @return  String representing current time in XSDdateTime format (null if error).
     */
    protected String getCurrentTimeInXSDDateTime() {
        try {
            final GregorianCalendar cal = new GregorianCalendar();
            cal.setTime(new Date());
            final XMLGregorianCalendar now = DatatypeFactory.newInstance().newXMLGregorianCalendar(cal);
            return now.toString();
        } catch (final DatatypeConfigurationException e) {
            LOGGER.error("Error converting date object to proper format!", e);
            return null;
        }
    }
}
