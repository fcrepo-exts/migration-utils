package org.fcrepo.migration.handlers;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.jena.atlas.io.IndentedWriter;
import org.fcrepo.client.FedoraContent;
import org.fcrepo.client.FedoraDatastream;
import org.fcrepo.client.FedoraException;
import org.fcrepo.client.FedoraObject;
import org.fcrepo.client.FedoraRepository;
import org.fcrepo.client.FedoraResource;
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.FedoraObjectVersionHandler;
import org.fcrepo.migration.MigrationIDMapper;
import org.fcrepo.migration.ObjectProperty;
import org.fcrepo.migration.ObjectReference;
import org.fcrepo.migration.ObjectVersionReference;
import org.fcrepo.migration.foxml11.DC;
import org.slf4j.Logger;

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

                if (object == null) {
                    object = createObject(version.getObject());
                }

                final QuadDataAcc triplesToInsert = new QuadDataAcc();
                final QuadAcc triplesToRemove = new QuadAcc();

                for (final DatastreamVersion v : version.listChangedDatastreams()) {
                    LOGGER.debug("Considering changed datastream version " + v.getVersionId());
                    if (v.getDatastreamInfo().getDatastreamId().equals("DC")) {
                        try {
                            final DC dc = DC.parseDC(v.getContent());
                            for (final String uri : dc.getRepresentedElementURIs()) {
                                triplesToRemove.addTriple(
                                        new Triple(NodeFactory.createURI(""),
                                                NodeFactory.createURI(uri),
                                                NodeFactory.createVariable("o")));
                                for (final String value : dc.getValuesForURI(uri)) {
                                    triplesToInsert.addTriple(
                                            new Triple(NodeFactory.createURI(""),
                                                    NodeFactory.createURI(uri),
                                                    NodeFactory.createLiteral(value)));
                                    LOGGER.debug("Adding " + uri + " value " + value);
                                }
                            }
                        } catch (final JAXBException e) {
                            throw new RuntimeException("Error parsing DC datastream " + v.getVersionId());
                        }
                    } else if (v.getDatastreamInfo().getDatastreamId().equals("RELS-EXT")) {
                        // migrate RELS-EXT
                        final String objectUri = "info:fedora/" + v.getDatastreamInfo().getObjectInfo().getPid();
                        final Model m = ModelFactory.createDefaultModel();
                        m.read(v.getContent(), null);
                        final StmtIterator statementIt = m.listStatements();
                        while (statementIt.hasNext()) {
                            final Statement s = statementIt.nextStatement();
                            if (s.getSubject().getURI().equals(objectUri)) {
                                final String predicateUri = s.getPredicate().getURI();
                                triplesToRemove.addTriple(new Triple(NodeFactory.createURI(""),
                                        NodeFactory.createURI(predicateUri),
                                        NodeFactory.createVariable("o")));
                                if (s.getObject().isLiteral()) {
                                    triplesToInsert.addTriple(
                                            new Triple(NodeFactory.createURI(""),
                                                    NodeFactory.createURI(predicateUri),
                                                    NodeFactory.createLiteral(
                                                            s.getObject().asLiteral().getString())));
                                } else if (s.getObject().isURIResource()) {
                                    triplesToInsert.addTriple(new Triple(NodeFactory.createURI(""),
                                            NodeFactory.createURI(predicateUri),
                                            NodeFactory.createURI(s.getObject().asResource().getURI())));
                                } else {
                                    throw new RuntimeException("No current handling for non-URI,"
                                            + " non-Literal subjects in Fedora RELS-EXT.");
                                }
                            } else {
                                throw new RuntimeException("Non-resource subject found: " + s.getSubject().getURI());
                            }
                        }
                    } else if ((v.getDatastreamInfo().getControlGroup().equals("E") && !importExternal)
                            || (v.getDatastreamInfo().getControlGroup().equals("R") && !importRedirect)) {
                        repo.createOrUpdateRedirectDatastream(
                                idMapper.mapDatastreamPath(v.getDatastreamInfo()), v.getExternalOrRedirectURL());
                    } else {
                        FedoraDatastream ds = dsMap.get(v.getDatastreamInfo().getDatastreamId());
                        if (ds == null) {
                            ds = repo.createDatastream(
                                    idMapper.mapDatastreamPath(v.getDatastreamInfo()),
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
     * Creates a Container in the Fedora 4 repository using the injected id
     * mapper.
     *
     * @param object            The ObjectReference from Fedora 3 to create in Fedora 4.
     * @throws FedoraException  In case there's an issue calling out to Fedora 4.
     * @return                  The newly created object.
     */
    protected FedoraObject createObject(final ObjectReference object) throws FedoraException {
        return repo.createObject(idMapper.mapObjectPath(object));
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
                mapObjectProperty(p, triplesToRemove, triplesToInsert);
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
     * WIP function to map object properties from 3 to 4.
     * Feel free to override this to suit your needs.
     *
     * @param p                 Object property to map from 3 to 4.
     * @param triplesToRemove   List of triples to remove from resource.
     * @param triplesToInsert   List of triples to add to resource.
     * @return                  void
     */
    protected void mapObjectProperty(final ObjectProperty p,
            final QuadAcc triplesToRemove,
            final QuadDataAcc triplesToInsert) {
        String pred = p.getName();
        final String obj = p.getValue();

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

        if (isDateProperty(p.getName())) {
            updateDateTriple(triplesToRemove,
                    triplesToInsert,
                    pred,
                    obj);
        } else {
            updateTriple(triplesToRemove,
                    triplesToInsert,
                    pred,
                    obj);
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
                updateTriple(triplesToRemove,
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
                updateTriple(triplesToRemove,
                        triplesToInsert,
                        "http://purl.org/dc/terms/title",
                        label);
            }

            // Object State
            final String state = v.getDatastreamInfo().getState();
            if (state != null) {
                updateTriple(triplesToRemove,
                        triplesToInsert,
                        "http://fedora.info/definitions/1/0/access/objState",
                        state);
            }

            // Format URI
            final String formatUri = v.getFormatUri();
            if (formatUri != null) {
                updateTriple(triplesToRemove,
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
    protected void updateTriple(final QuadAcc triplesToRemove,
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
     * Utility function for updating a date literal triple.
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
