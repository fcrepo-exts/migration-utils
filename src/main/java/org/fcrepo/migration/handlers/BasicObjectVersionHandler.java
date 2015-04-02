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

                        //
                        // Handle datastream properties
                        //
                        QuadDataAcc dsTriplesToInsert = new QuadDataAcc();
                        QuadAcc dsTriplesToRemove = new QuadAcc();

                        // DSID
                        String dsid = v.getDatastreamInfo().getDatastreamId();
                        if (dsid != null) {
                            dsTriplesToRemove.addTriple(new Triple(NodeFactory.createVariable("s"),
                                                                   NodeFactory.createURI("http://purl.org/dc/terms/identifier"),
                                                                   NodeFactory.createVariable("o")));
                            dsTriplesToInsert.addTriple(new Triple(NodeFactory.createURI(""),
                                                                   NodeFactory.createURI("http://purl.org/dc/terms/identifier"),
                                                                   NodeFactory.createLiteral(dsid)));
                        }

                        // Label
                        String label = v.getLabel();
                        if (label != null) {
                            dsTriplesToRemove.addTriple(new Triple(NodeFactory.createVariable("s"),
                                                                   NodeFactory.createURI("http://purl.org/dc/terms/title"),
                                                                   NodeFactory.createVariable("o")));
                            dsTriplesToInsert.addTriple(new Triple(NodeFactory.createURI(""),
                                                                   NodeFactory.createURI("http://purl.org/dc/terms/title"),
                                                                   NodeFactory.createLiteral(label)));
                        }

                        // Object State 
                        String state = v.getDatastreamInfo().getState();
                        if (state != null) {
                            dsTriplesToRemove.addTriple(new Triple(NodeFactory.createVariable("s"),
                                                                   NodeFactory.createURI("http://fedora.info/definitions/1/0/access/objState"),
                                                                   NodeFactory.createVariable("o")));
                            dsTriplesToInsert.addTriple(new Triple(NodeFactory.createURI(""),
                                                                   NodeFactory.createURI("http://fedora.info/definitions/1/0/access/objState"),
                                                                   NodeFactory.createLiteral(state)));
                        }

                        // Created Date
                        String createdDate = v.getCreated();
                        if (createdDate != null) {
                            dsTriplesToRemove.addTriple(new Triple(NodeFactory.createVariable("s"),
                                                                   NodeFactory.createURI("http://www.loc.gov/premis/rdf/v1#hasDateCreatedByApplication"),
                                                                   NodeFactory.createVariable("o")));
                            dsTriplesToInsert.addTriple(new Triple(NodeFactory.createURI(""),
                                                                   NodeFactory.createURI("http://www.loc.gov/premis/rdf/v1#hasDateCreatedByApplication"),
                                                                   NodeFactory.createLiteral(createdDate, XSDDatatype.XSDdateTime)));
                        }

                        // Format URI 
                        String formatUri = v.getFormatUri();
                        if (formatUri != null) {
                            dsTriplesToRemove.addTriple(new Triple(NodeFactory.createVariable("s"),
                                                                   NodeFactory.createURI("http://www.loc.gov/premis/rdf/v1#formatDesignation"),
                                                                   NodeFactory.createVariable("o")));
                            dsTriplesToInsert.addTriple(new Triple(NodeFactory.createURI(""),
                                                                   NodeFactory.createURI("http://www.loc.gov/premis/rdf/v1#formatDesignation"),
                                                                   NodeFactory.createLiteral(formatUri)));
                        }

                        UpdateRequest dsUpdateRequest = UpdateFactory.create();
                        dsUpdateRequest.add(new UpdateDeleteWhere(dsTriplesToRemove));
                        dsUpdateRequest.add(new UpdateDataInsert(dsTriplesToInsert));
                        ByteArrayOutputStream dsSparqlUpdate = new ByteArrayOutputStream();
                        dsUpdateRequest.output(new IndentedWriter(dsSparqlUpdate));
                        ds.updateProperties(dsSparqlUpdate.toString("UTF-8"));
                    }
                }

                if (version.isLastVersion()) {
                    for (ObjectProperty p : version.getObjectProperties().listProperties()) {
                        triplesToRemove.addTriple(new Triple(NodeFactory.createVariable("s"), NodeFactory.createURI(p.getName()), NodeFactory.createVariable("o")));
                        triplesToInsert.addTriple(new Triple(NodeFactory.createURI(""), NodeFactory.createURI(p.getName()),
                                isDateProperty(p.getName())
                                        ? NodeFactory.createLiteral(p.getValue(), XSDDatatype.XSDdateTime)
                                        : NodeFactory.createLiteral(p.getValue())));
                    }
                }

                // update the version date
                triplesToRemove.addTriple(new Triple(NodeFactory.createVariable("s"), NodeFactory.createURI("http://www.loc.gov/premis/rdf/v1#hasDateCreatedByApplication"), NodeFactory.createVariable("o")));
                triplesToInsert.addTriple(new Triple(NodeFactory.createURI(""), NodeFactory.createURI("http://www.loc.gov/premis/rdf/v1#hasDateCreatedByApplication"), NodeFactory.createLiteral(version.getVersionDate(), XSDDatatype.XSDdateTime)));

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

}
