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
import org.fcrepo.client.FedoraContent;
import org.fcrepo.client.FedoraDatastream;
import org.fcrepo.client.FedoraException;
import org.fcrepo.client.FedoraObject;
import org.fcrepo.client.FedoraRepository;
import org.fcrepo.client.FedoraResource;
import org.fcrepo.kernel.RdfLexicon;
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.FedoraObjectVersionHandler;
import org.fcrepo.migration.MigrationIDMapper;
import org.fcrepo.migration.ObjectProperty;
import org.fcrepo.migration.ObjectReference;
import org.fcrepo.migration.ObjectVersionReference;
import org.fcrepo.migration.foxml11.DC;
import org.slf4j.Logger;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

/**
 *
 * @author mdurbin
 *
 */
public class BasicObjectVersionHandler implements FedoraObjectVersionHandler {

    private static Logger LOGGER = getLogger(BasicObjectVersionHandler.class);

    private static int suffix = 0;

    private FedoraRepository repo;

    private MigrationIDMapper idMapper;

    private boolean importExternal;

    private boolean importRedirect;

    /**
     * Basic object version handler.
     * @param repo the fedora repository
     * @param idMapper the id mapper
     */
    public BasicObjectVersionHandler(final FedoraRepository repo, final MigrationIDMapper idMapper) {
        this.repo = repo;
        this.idMapper = idMapper;
    }

    /**
     * A property setter for a property that determines the handling of External (X)
     * fedora 3 datastreams.  If true, the content of the URL to which those datastreams
     * redirect is fetched and ingested as a fedora 4-managed non-RDF resource.  If false
     * (default), a non-RDF resource is created in fedora 4 that when fetched results in
     * an HTTP redirect to the external url.
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
     */
    public void setImportRedirect(final boolean value) {
        this.importRedirect = value;
    }

    @Override
    public void processObjectVersions(final Iterable<ObjectVersionReference> versions) {
        FedoraObject object = null;
        final Map<String, FedoraDatastream> dsMap = new HashMap<String, FedoraDatastream>();

        try {
            for (final ObjectVersionReference version : versions) {

                LOGGER.debug("Considering object "
                        + version.getObjectInfo().getPid()
                        + " version at " + version.getVersionDate() + ".");

                final String objectPath = idMapper.mapObjectPath(version.getObjectInfo().getPid());
                if (object == null) {
                    if (repo.exists(objectPath)) {
                        object = repo.getObject(objectPath);
                        if (!isPlaceholder(object)) {
                            throw new RuntimeException("An object already exists at \"" + objectPath + "\"!");
                        }
                    } else {
                        object = repo.createObject(objectPath);
                    }
                }

                final QuadDataAcc triplesToInsert = new QuadDataAcc();
                final QuadAcc triplesToRemove = new QuadAcc();

                for (final DatastreamVersion v : version.listChangedDatastreams()) {
                    LOGGER.debug("Considering changed datastream version " + v.getVersionId());
                    if (v.getDatastreamInfo().getDatastreamId().equals("DC")) {
                        migrateDc(v, triplesToRemove, triplesToInsert);
                    } else if (v.getDatastreamInfo().getDatastreamId().equals("RELS-EXT")) {
                        migrateRelsExt(v, triplesToRemove, triplesToInsert);
                    } else if (v.getDatastreamInfo().getDatastreamId().equals("RELS-INT")) {
                        migrateRelsInt(v, dsMap);
                    } else if ((v.getDatastreamInfo().getControlGroup().equals("E") && !importExternal)
                            || (v.getDatastreamInfo().getControlGroup().equals("R") && !importRedirect)) {
                        repo.createOrUpdateRedirectDatastream(
                                idMapper.mapDatastreamPath(v.getDatastreamInfo().getObjectInfo().getPid(),
                                        v.getDatastreamInfo().getDatastreamId()), v.getExternalOrRedirectURL());
                    } else {
                        FedoraDatastream ds = dsMap.get(v.getDatastreamInfo().getDatastreamId());
                        if (ds == null) {
                            ds = repo.createDatastream(
                                    idMapper.mapDatastreamPath(v.getDatastreamInfo().getObjectInfo().getPid(),
                                            v.getDatastreamInfo().getDatastreamId()),
                                    new FedoraContent().setContent(v.getContent()).setContentType(v.getMimeType()));
                            dsMap.put(v.getDatastreamInfo().getDatastreamId(), ds);
                        } else {
                            ds.updateContent(
                                    new FedoraContent().setContent(v.getContent()).setContentType(v.getMimeType()));
                        }
                        updateDatastreamProperties(version.getObject(), v, ds);
                    }
                }

                updateObjectProperties(version, object, triplesToRemove, triplesToInsert);

                object.createVersionSnapshot("imported-version-" + String.valueOf(version.getVersionIndex()));
            }
        } catch (final FedoraException e) {
            throw new RuntimeException(e);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
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
     * @param object            Object to update
     * @param triplesToRemove   List of triples to remove from resource.
     * @param triplesToInsert   List of triples to add to resource.
     * @return                  void
     */
    protected void updateObjectProperties(final ObjectVersionReference version,
            final FedoraObject object,
            final QuadAcc triplesToRemove,
            final QuadDataAcc triplesToInsert) throws FedoraException {
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
            updateResourceProperties(object, triplesToRemove, triplesToInsert);
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
     * @return                  void
     */
    protected void mapProperty(final String origPred,
                               final String obj,
                               final QuadAcc triplesToRemove,
                               final QuadDataAcc triplesToInsert,
                               final Boolean isLiteral) throws FedoraException {
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
     * @param v     Version of the datasream to update.
     * @param ds    Datastream to update.
     * @return void
     */
    protected void updateDatastreamProperties(final ObjectReference obj,
            final DatastreamVersion v, final FedoraDatastream ds) {
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
            updateResourceProperties(ds, triplesToRemove, triplesToInsert);
        }
    }

    /**
     * Migrates a RELS-EXT datastream by splitting it apart into triples to
     * update on the object it describes.
     *
     * @param v                 Version of the datasream to migrate.
     * @param triplesToRemove   List of triples to remove from resource.
     * @param triplesToInsert   List of triples to add to resource.
     * @return                  void
     */
    protected void migrateRelsExt(final DatastreamVersion v, final QuadAcc triplesToRemove,
                                  final QuadDataAcc triplesToInsert) throws IOException, FedoraException {
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
     * @param dsMap Map of datastreams indexed by label.
     * @return      void
     */
    protected void migrateRelsInt(final DatastreamVersion v, final Map<String, FedoraDatastream> dsMap)
            throws IOException, RuntimeException, FedoraException {
        // Read the RDF.
        final Model m = ModelFactory.createDefaultModel();
        m.read(v.getContent(), null);
        final StmtIterator statementIt = m.listStatements();
        while (statementIt.hasNext()) {
            // Get the datastream this triple describes.
            final Statement s = statementIt.nextStatement();
            final String dsUri = s.getSubject().getURI();
            final String[] splitUri = dsUri.split("/");
            final String dsLabel = splitUri[splitUri.length - 1];
            final FedoraDatastream ds = dsMap.get(dsLabel);

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

            updateResourceProperties(ds, triplesToRemove, triplesToInsert);
        }
    }

    /**
     * Migrates a DC datastream by shredding it into RDF properties and
     * applying them directly to the object.
     *
     * @param v                 Version of the datasream to migrate.
     * @param triplesToRemove   List of triples to remove from resource.
     * @param triplesToInsert   List of triples to add to resource.
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
     * Utility function for udpating a FedoraResource's properties.
     *
     * @param resource          FedoraResource to update.
     * @param triplesToRemove   List of triples to remove from resource.
     * @param triplesToInsert   List of triples to add to resource.
     * @return                  void
     * @throws RuntimeException Possible FedoraExcpetions and IOExceptions
     */
    protected void updateResourceProperties(final FedoraResource resource,
            final QuadAcc triplesToRemove,
            final QuadDataAcc triplesToInsert) throws RuntimeException {
        try {
            final UpdateRequest updateRequest = UpdateFactory.create();
            updateRequest.setPrefix("dcterms", "http://purl.org/dc/terms/");
            updateRequest.setPrefix("fedoraaccess", "http://fedora.info/definitions/1/0/access/");
            updateRequest.setPrefix("fedora3model", "info:fedora/fedora-system:def/model#");
            updateRequest.add(new UpdateDeleteWhere(triplesToRemove));
            updateRequest.add(new UpdateDataInsert(triplesToInsert));
            final ByteArrayOutputStream sparqlUpdate = new ByteArrayOutputStream();
            updateRequest.output(new IndentedWriter(sparqlUpdate));
            LOGGER.trace("SPARQL: " + sparqlUpdate.toString("UTF-8"));
            resource.updateProperties(sparqlUpdate.toString("UTF-8"));
            suffix = 0;
        } catch (final FedoraException e) {
            throw new RuntimeException(e);
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
     * @return                  void
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
     * @return                  void
     */
    protected void updateUriTriple(final QuadAcc triplesToRemove,
                                   final QuadDataAcc triplesToInsert,
                                   final String predicate,
                                   final String object) throws FedoraException {
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
     */
    protected String resolveInternalURI(final String uri) throws FedoraException {
        if (uri.startsWith("info:fedora/")) {
            final String path = idMapper.mapObjectPath(uri.substring("info:fedora/".length()));
            createPlaceholder(path);
            return repo.getRepositoryUrl() + path;
        }
        return uri;
    }

    /**
     * Creates an empty object in fedora 4 at the given path such that
     * relationships to that object from other objects may be created before
     * that object is migrated.
     */
    protected void createPlaceholder(final String path) throws FedoraException {
        if (!repo.exists(path)) {
            repo.createObject(path);
        }
    }

    /**
     * Determines whether the given fedora object is a placeholder object
     * created earlier to support the inclusion of a relationship to that
     * object (rather than a previously existing object).  The current
     * implementation bases it's assessment on whether any version
     * snapshots have been made, since they are expected to be made for
     * all migrated objects and NOT for any placeholder objects.
     */
    protected boolean isPlaceholder(final FedoraObject o) throws FedoraException {
        final Iterator<Triple> properties = o.getProperties();
        while (properties.hasNext()) {
            final Triple t = properties.next();
            if (t.predicateMatches(RdfLexicon.HAS_VERSION_HISTORY.asNode())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Utility function for updating a date triple.
     *
     * @param triplesToRemove   List of triples to remove from resource.
     * @param triplesToInsert   List of triples to add to resource.
     * @param predicate         Predicate of relationship (assumed to be URI).
     * @param object            Object of relationship (assumed to be literal).
     * @return                  void
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
     * @return                  void
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
