/*
 * Copyright 2019 DuraSpace, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fcrepo.migration.handlers.ocfl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.FedoraObjectVersionHandler;
import org.fcrepo.migration.ObjectReference;
import org.fcrepo.migration.ObjectVersionReference;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Writes a Fedora object as a single ArchiveGroup.
 * <p>
 * All datastreams and object metadata from a fcrepo3 object are persisted to a
 * single OCFL object (ArchiveGroup in fcrepo6 parlance).
 * </p>
 * <p>
 * The contents of each datastream are written verbatim. No attempt is made to
 * re-write the RELS-EXT to replace subjects and objects with their LDP
 * counterparts.
 * </p>
 * <p>
 * Note: fedora-specific OCFL serialization features (such as redirects,
 * container metadata, etc) is not fully defined yet, so are not included here
 *
 * @author apb@jhu.edu
 */
public class ArchiveGroupHandler
implements FedoraObjectVersionHandler {

    final OcflDriver driver;

    /**
     * Create an ArchiveGroupHandler,
     *
     * @param driver
     *        OCFL driver
     */
    public ArchiveGroupHandler(final OcflDriver driver) {
        this.driver = driver;
    }

    @Override
    public void processObjectVersions(final Iterable<ObjectVersionReference> versions) {
        // We need to manually keep track of the datastream creation dates
        final Map<String, String> dsCreateDates = new HashMap<String, String>();

        versions.forEach(ov -> {

            // We use the PID to identify the OCFL object
            final String id = ov.getObjectInfo().getPid();
            final OcflSession session = driver.Open(id);

            // Object properties are written only once (as fcrepo3 object properties were
            // unversioned). Note: the OCFL object format for Fedora resources or
            // ArchiveGroups is not defined,
            // so this is a bit of a stab in the dark.
            if (ov.isFirstVersion()) {
                session.put("object.ttl", getObjTriples(ov));
            }

            // Write datastreams and their metadata
            ov.listChangedDatastreams().forEach(dv -> {
                final String dsid = dv.getDatastreamInfo().getDatastreamId();

                // For redirect or external, lust record the URI for now as the "datastream content".
                // Fedora's representation of redirect or external within OCFL objects hasn't been
                // defined yet.
                if ("RE".contains(dv.getDatastreamInfo().getControlGroup())) {
                    session.put(dsid,
                                IOUtils.toInputStream(dv
                                                      .getExternalOrRedirectURL()));
                } else {

                    /*
                     * Write datastream itself, catching that silly IOExeption
                     */
                    try {
                        session.put(dsid, dv.getContent());
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                /* Write metadata */
                session.put(dsid + ".ttl",
                            getDsTriples(dv, ov.getObject(), dsCreateDates));
            });

            session.commit();
        });
    }

    // Get object-level triples
    private static InputStream getObjTriples(final ObjectVersionReference o) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Model triples = ModelFactory.createDefaultModel();
        final String uri = "info:fedora/" + o.getObjectInfo().getPid();

        o.getObjectProperties().listProperties().forEach(p -> {
            if (p.getName().contains("Date")) {
                addDateLiteral(triples, uri, p.getName(), p.getValue());
            } else {
                addStringLiteral(triples, uri, p.getName(), p.getValue());
            }
        });

        triples.write(out, "TTL");
        return new ByteArrayInputStream(out.toByteArray());
    }

    // Get datastream-level triples
    private static InputStream getDsTriples(final DatastreamVersion dv,
                                            final ObjectReference ref,
                                            final Map<String, String> createDates) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Model triples = ModelFactory.createDefaultModel();

        final String uri =
                "info:fedora/" + dv.getDatastreamInfo().getObjectInfo().getPid()
                        + "/" + dv.getDatastreamInfo().getDatastreamId();
        final String dsid = dv.getDatastreamInfo().getDatastreamId();

        System.out.println("DS URI: " + uri);

        if (dv.isFirstVersionIn(ref)) {
            createDates.put(dv.getDatastreamInfo().getDatastreamId(),
                            dv.getCreated());
        }

        addDateLiteral(triples,
                       uri,
                       "http://fedora.info/definitions/v4/repository#created",
                       createDates.get(dsid));
        addDateLiteral(triples,
                       uri,
                       "http://fedora.info/definitions/v4/repository#lastModified",
                       dv.getCreated());
        addStringLiteral(triples,
                         uri,
                         "http://purl.org/dc/terms/identifier",
                         dv.getDatastreamInfo().getDatastreamId());
        addStringLiteral(triples,
                         uri,
                         "http://purl.org/dc/terms/title",
                         dv.getLabel());
        addStringLiteral(triples,
                         uri,
                         "http://fedora.info/definitions/1/0/access/objState",
                         dv.getDatastreamInfo().getState());
        addStringLiteral(triples,
                         uri,
                         "http://www.loc.gov/premis/rdf/v1#formatDesignation",
                         dv.getFormatUri());
        addStringLiteral(triples,
                         uri,
                         "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#hasMimeType",
                         dv.getMimeType());

        triples.write(out, "TTL");
        return new ByteArrayInputStream(out.toByteArray());
    }

    private static void addStringLiteral(final Model m,
                                         final String s,
                                         final String p,
                                         final String o) {
        if (o != null) {
            m.add(m.createResource(s), m.createProperty(p), o);
        }
    }

    private static void addDateLiteral(final Model m,
                                       final String s,
                                       final String p,
                                       final String date) {
        if (date != null) {
            m.addLiteral(m.createResource(s),
                         m.createProperty(p),
                         m.createTypedLiteral(date, XSDDatatype.XSDdateTime));
        }
    }
}
