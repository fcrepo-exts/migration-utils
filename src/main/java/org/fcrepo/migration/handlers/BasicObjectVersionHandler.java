package org.fcrepo.migration.handlers;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
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
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.FedoraObjectVersionHandler;
import org.fcrepo.migration.MigrationIDMapper;
import org.fcrepo.migration.ObjectProperty;
import org.fcrepo.migration.ObjectReference;
import org.fcrepo.migration.ObjectVersionReference;
import org.fcrepo.migration.foxml11.DC;
import org.slf4j.Logger;

import javax.xml.bind.JAXBException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

public class BasicObjectVersionHandler implements FedoraObjectVersionHandler {

    private static Logger LOGGER = getLogger(BasicObjectVersionHandler.class);
    
    private FedoraRepository repo;

    private MigrationIDMapper idMapper;

    public BasicObjectVersionHandler(FedoraRepository repo, MigrationIDMapper idMapper) {
        this.repo = repo;
        this.idMapper = idMapper;
    }

    @Override
    public void processObjectVersions(Iterable<ObjectVersionReference> versions) {
        FedoraObject object = null;
        Map<String, FedoraDatastream> dsMap = new HashMap<String, FedoraDatastream>();
        
        try {
            for (ObjectVersionReference version : versions) {

                LOGGER.debug("Considering object " + version.getObjectInfo().getPid() + " version at " + version.getVersionDate() + ".");

                if (object == null) {
                    object = createObject(version.getObject());
                }

                QuadDataAcc triplesToInsert = new QuadDataAcc();
                QuadAcc triplesToRemove = new QuadAcc();

                for (DatastreamVersion v : version.listChangedDatastreams()) {
                    LOGGER.debug("Considering changed datastream version " + v.getVersionId());
                    if (v.getDatastreamInfo().getDatastreamId().equals("DC")) {
                        try {
                            DC dc = DC.parseDC(v.getContent());
                            for (String uri : dc.getRepresentedElementURIs()) {
                                triplesToRemove.addTriple(new Triple(NodeFactory.createURI(""), NodeFactory.createURI(uri), NodeFactory.createVariable("o")));
                                for (String value : dc.getValuesForURI(uri)) {
                                    triplesToInsert.addTriple(new Triple(NodeFactory.createURI(""), NodeFactory.createURI(uri), NodeFactory.createLiteral(value)));
                                    LOGGER.debug("Adding " + uri + " value " + value);
                                }
                            }
                        } catch (JAXBException e) {
                            throw new RuntimeException("Error parsing DC datastream " + v.getVersionId());
                        }
                    } else if (v.getDatastreamInfo().getDatastreamId().equals("RELS-EXT")) {
                        // migrate RELS-EXT
                        final String objectUri = "info:fedora/" + v.getDatastreamInfo().getObjectInfo().getPid();
                        Model m = ModelFactory.createDefaultModel();
                        m.read(v.getContent(), null);
                        StmtIterator statementIt = m.listStatements();
                        while (statementIt.hasNext()) {
                            Statement s = statementIt.nextStatement();
                            if (s.getSubject().getURI().equals(objectUri)) {
                                final String predicateUri = s.getPredicate().getURI();
                                triplesToRemove.addTriple(new Triple(NodeFactory.createURI(""), NodeFactory.createURI(predicateUri), NodeFactory.createVariable("o")));
                                if (s.getObject().isLiteral()) {
                                    triplesToInsert.addTriple(new Triple(NodeFactory.createURI(""), NodeFactory.createURI(predicateUri), NodeFactory.createLiteral(s.getObject().asLiteral().getString())));
                                } else if (s.getObject().isURIResource()) {
                                    triplesToInsert.addTriple(new Triple(NodeFactory.createURI(""), NodeFactory.createURI(predicateUri), NodeFactory.createURI(s.getObject().asResource().getURI())));
                                } else {
                                    throw new RuntimeException("No current handling for non-URI, non-Literal subjects in Fedora RELS-EXT.");
                                }
                            } else {
                                throw new RuntimeException("Non-resource subject found: " + s.getSubject().getURI());
                            }
                        }
                    } else if (v.getDatastreamInfo().getControlGroup().equals("E")) {
                        // TODO: handle external datastreams
                    } else if (v.getDatastreamInfo().getControlGroup().equals("R")) {
                        // TODO: handle redirect datastreams
                    } else {
                        FedoraDatastream ds = dsMap.get(v.getDatastreamInfo().getDatastreamId());
                        if (ds == null) {
                            ds = repo.createDatastream(idMapper.mapDatastreamPath(v.getDatastreamInfo()),
                                                       new FedoraContent().setContent(v.getContent()).setContentType(v.getMimeType()));
                            dsMap.put(v.getDatastreamInfo().getDatastreamId(), ds);
                        } else {
                            ds.updateContent(new FedoraContent().setContent(v.getContent()).setContentType(v.getMimeType()));
                        }

                        updateDatastreamProperties(v, ds);
                    }
                }

                if (version.isLastVersion()) {
                    for (ObjectProperty p : version.getObjectProperties().listProperties()) {
                        if (isDateProperty(p.getName())) {
                            updateDateTriple(triplesToRemove,
                                             triplesToInsert,
                                             p.getName(),
                                             p.getValue());
                        }
                        else {
                            updateTriple(triplesToRemove,
                                         triplesToInsert,
                                         p.getName(),
                                         p.getValue());
                        }
                    }
                }

                // update the version date
                updateDateTriple(triplesToRemove,
                                 triplesToInsert,
                                 "http://www.loc.gov/premis/rdf/v1#hasDateCreatedByApplication",
                                 version.getVersionDate());

                UpdateRequest request = UpdateFactory.create();
                request.add(new UpdateDeleteWhere(triplesToRemove));
                request.add(new UpdateDataInsert(triplesToInsert));
                ByteArrayOutputStream sparqlUpdate = new ByteArrayOutputStream();
                request.output(new IndentedWriter(sparqlUpdate));
                object.updateProperties(sparqlUpdate.toString("UTF-8"));

                object.createVersionSnapshot("imported-version-" + String.valueOf(version.getVersionIndex()));
            }
        } catch (FedoraException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private boolean isDateProperty(String uri) {
        return uri.equals("info:fedora/fedora-system:def/model#createdDate") || uri.equals("info:fedora/fedora-system:def/view#lastModifiedDate");
        
    }

    private FedoraObject createObject(ObjectReference object) throws FedoraException {
        return repo.createObject(idMapper.mapObjectPath(object));
    }

    private void updateDatastreamProperties(DatastreamVersion v, FedoraDatastream ds) {
        QuadDataAcc triplesToInsert = new QuadDataAcc();
        QuadAcc triplesToRemove = new QuadAcc();

        // DSID
        String dsid = v.getDatastreamInfo().getDatastreamId();
        if (dsid != null) {
            updateTriple(triplesToRemove,
                         triplesToInsert,
                         "http://purl.org/dc/terms/identifier",
                         dsid);
        }

        // Label
        String label = v.getLabel();
        if (label != null) {
            updateTriple(triplesToRemove,
                         triplesToInsert,
                         "http://purl.org/dc/terms/title",
                         label);
        }

        // Object State 
        String state = v.getDatastreamInfo().getState();
        if (state != null) {
            updateTriple(triplesToRemove,
                         triplesToInsert,
                         "http://fedora.info/definitions/1/0/access/objState",
                         state);
        }

        // Created Date
        String createdDate = v.getCreated();
        if (createdDate != null) {
            updateDateTriple(triplesToRemove,
                             triplesToInsert,
                             "http://www.loc.gov/premis/rdf/v1#hasDateCreatedByApplication",
                             createdDate);
        }

        // Format URI 
        String formatUri = v.getFormatUri();
        if (formatUri != null) {
            updateTriple(triplesToRemove,
                         triplesToInsert,
                         "http://www.loc.gov/premis/rdf/v1#formatDesignation",
                         formatUri);
        }

        updateResourceProperties(ds, triplesToRemove, triplesToInsert);
    }

    private void updateResourceProperties(FedoraResource resource, QuadAcc triplesToRemove, QuadDataAcc triplesToInsert) throws RuntimeException {
        try {
            UpdateRequest updateRequest = UpdateFactory.create();
            updateRequest.add(new UpdateDeleteWhere(triplesToRemove));
            updateRequest.add(new UpdateDataInsert(triplesToInsert));
            ByteArrayOutputStream sparqlUpdate = new ByteArrayOutputStream();
            updateRequest.output(new IndentedWriter(sparqlUpdate));
            resource.updateProperties(sparqlUpdate.toString("UTF-8"));
        } catch (FedoraException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateTriple(QuadAcc triplesToRemove, QuadDataAcc triplesToInsert, String predicate, String object) {
        triplesToRemove.addTriple(new Triple(NodeFactory.createVariable("s"),
                                             NodeFactory.createURI(predicate),
                                             NodeFactory.createVariable("o")));
        triplesToInsert.addTriple(new Triple(NodeFactory.createURI(""),
                                             NodeFactory.createURI(predicate),
                                             NodeFactory.createLiteral(object)));
    }

    private void updateDateTriple(QuadAcc triplesToRemove, QuadDataAcc triplesToInsert, String predicate, String object) {
        triplesToRemove.addTriple(new Triple(NodeFactory.createVariable("s"),
                                             NodeFactory.createURI(predicate),
                                             NodeFactory.createVariable("o")));
        triplesToInsert.addTriple(new Triple(NodeFactory.createURI(""),
                                             NodeFactory.createURI(predicate),
                                             NodeFactory.createLiteral(object, XSDDatatype.XSDdateTime)));
    }
}
