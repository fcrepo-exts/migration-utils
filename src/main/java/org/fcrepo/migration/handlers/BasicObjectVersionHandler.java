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
import java.util.List;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

public class BasicObjectVersionHandler implements FedoraObjectVersionHandler {

    private static Logger LOGGER = getLogger(BasicObjectVersionHandler.class);

    private FedoraRepository repo;

    private MigrationIDMapper idMapper;

    private boolean importExternal;

    private boolean importRedirect;

    public BasicObjectVersionHandler(FedoraRepository repo, MigrationIDMapper idMapper) {
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
    public void setImportExternal(boolean value) {
        this.importExternal = value;
    }

    /**
     * A property setter for a property that determines the handling of Redirect (R)
     * fedora 3 datastreams.  If true, the content of the URL to which those datastreams
     * redirect is fetched and ingested as a fedora 4-managed non-RDF resource.  If false
     * (default), a non-RDF resource is created in fedora 4 that when fetched results in
     * an HTTP redirect to the external url.
     */
    public void setImportRedirect(boolean value) {
        this.importRedirect = value;
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
                    } else if ((v.getDatastreamInfo().getControlGroup().equals("E") && !importExternal)
                            || (v.getDatastreamInfo().getControlGroup().equals("R") && !importRedirect)) {
                        FedoraDatastream ds = repo.createOrUpdateRedirectDatastream(idMapper.mapDatastreamPath(v.getDatastreamInfo()), v.getExternalOrRedirectURL());
                    } else {
                        FedoraDatastream ds = dsMap.get(v.getDatastreamInfo().getDatastreamId());
                        if (ds == null) {
                            dsMap.put(v.getDatastreamInfo().getDatastreamId(), repo.createDatastream(idMapper.mapDatastreamPath(v.getDatastreamInfo()), new FedoraContent().setContent(v.getContent()).setContentType(v.getMimeType())));
                        } else {
                            ds.updateContent(new FedoraContent().setContent(v.getContent()).setContentType(v.getMimeType()));
                        }
                        // TODO: handle datastream properties
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
