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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.FedoraObjectVersionHandler;
import org.fcrepo.migration.MigrationType;
import org.fcrepo.migration.ObjectVersionReference;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static org.slf4j.LoggerFactory.getLogger;

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
public class ArchiveGroupHandler implements FedoraObjectVersionHandler {

    private static final Logger LOGGER = getLogger(ArchiveGroupHandler.class);

    private static final String FCREPO_ROOT = "info:fedora/";
    static final String BASIC_CONTAINER = "http://www.w3.org/ns/ldp#BasicContainer";
    static final String NON_RDF_SOURCE = "http://www.w3.org/ns/ldp#NonRDFSource";
    static final String NON_RDF_SOURCE_DESCRIPTION =
            "http://fedora.info/definitions/v4/repository#NonRdfSourceDescription";
    private static final String FCREPO_DIR = ".fcrepo/";
    private static final String DESC_SUFFIX = "-description";
    private static final String RDF_EXTENSION = ".nt";

    private static final Map<String, String> externalHandlingMap = Map.of(
            "E", "proxy",
            "R", "redirect"
    );

    private final OcflDriver driver;
    private final boolean addDatastreamExtensions;
    private final MigrationType migrationType;
    private final String user;

    private final ObjectMapper objectMapper;
    private final Detector mimeDetector;

    /**
     * Create an ArchiveGroupHandler,
     *
     * @param driver
     *        OCFL driver
     * @param migrationType
     *        the type of migration to do
     * @param addDatastreamExtensions
     *        true if datastreams should be written with file extensions
     * @param user
     *        the username to associated with the migrated resources
     */
    public ArchiveGroupHandler(final OcflDriver driver,
                               final MigrationType migrationType,
                               final boolean addDatastreamExtensions,
                               final String user) {
        this.driver = Preconditions.checkNotNull(driver, "driver cannot be null");
        this.migrationType = Preconditions.checkNotNull(migrationType, "migrationType cannot be null");
        this.addDatastreamExtensions = addDatastreamExtensions;
        this.user = Preconditions.checkNotNull(Strings.emptyToNull(user), "user cannot be blank");
        this.objectMapper = new ObjectMapper()
                .configure(WRITE_DATES_AS_TIMESTAMPS, false)
                .registerModule(new JavaTimeModule())
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
        try {
            this.mimeDetector = new TikaConfig().getDetector();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void processObjectVersions(final Iterable<ObjectVersionReference> versions) {
        // We need to manually keep track of the datastream creation dates
        final Map<String, String> dsCreateDates = new HashMap<>();

        versions.forEach(ov -> {

            // We use the PID to identify the OCFL object
            final String objectId = ov.getObjectInfo().getPid();
            final String f6ObjectId = FCREPO_ROOT + objectId;
            final OcflSession session = driver.open(f6ObjectId);

            // Object properties are written only once (as fcrepo3 object properties were unversioned).
            if (ov.isFirstVersion()) {
                writeObjectFiles(f6ObjectId, ov, session);
            }

            // Write datastreams and their metadata
            ov.listChangedDatastreams().forEach(dv -> {
                final var mimeType = resolveMimeType(dv);
                final String dsId = dv.getDatastreamInfo().getDatastreamId();
                final String f6DsId = resolveF6DatastreamId(dsId, f6ObjectId, mimeType);
                final var datastreamFilename = lastPartFromId(f6DsId);

                if (dv.isFirstVersionIn(ov.getObject())) {
                    dsCreateDates.put(dsId, dv.getCreated());
                }
                final var createDate = dsCreateDates.get(dsId);

                final var datastreamHeaders = createDatastreamHeaders(dv, f6DsId, f6ObjectId,
                        datastreamFilename, mimeType, createDate);

                if ("RE".contains(dv.getDatastreamInfo().getControlGroup())) {
                    // Write a file for external content only for vanilla OCFL migration
                    if (migrationType == MigrationType.VANILLA_OCFL) {
                        session.put(datastreamFilename, IOUtils.toInputStream(dv.getExternalOrRedirectURL()));
                    }
                } else {
                    try (var content = dv.getContent()) {
                        session.put(datastreamFilename, content);
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }

                    writeDescriptionFiles(f6DsId, datastreamFilename, createDate, datastreamHeaders, dv, session);
                }

                if (migrationType == MigrationType.F6_OCFL) {
                    session.put(headersFilename(datastreamFilename), serializeHeaders(datastreamHeaders));
                }
            });

            session.commit();
        });
    }

    private void writeObjectFiles(final String f6ObjectId,
                                  final ObjectVersionReference ov,
                                  final OcflSession session) {
        final var lastPart = lastPartFromId(f6ObjectId);
        final var objectHeaders = createObjectHeaders(f6ObjectId, ov);
        session.put(lastPart + RDF_EXTENSION, getObjTriples(ov));
        if (migrationType == MigrationType.F6_OCFL) {
            session.put(headersFilename(lastPart), serializeHeaders(objectHeaders));
        }
    }

    private void writeDescriptionFiles(final String f6Dsid,
                                       final String datastreamFilename,
                                       final String createDate,
                                       final ResourceHeaders datastreamHeaders,
                                       final DatastreamVersion dv,
                                       final OcflSession session) {
        final var descBaseName = datastreamFilename + DESC_SUFFIX;
        session.put(descBaseName + RDF_EXTENSION, getDsTriples(dv, f6Dsid, createDate));

        if (migrationType == MigrationType.F6_OCFL) {
            final var descriptionHeaders = createDescriptionHeaders(f6Dsid, datastreamFilename, datastreamHeaders);
            session.put(headersFilename(descBaseName), serializeHeaders(descriptionHeaders));
        }
    }

    private String f6DescriptionId(final String f6ResourceId) {
        return f6ResourceId + "/fcr:metadata";
    }

    private String lastPartFromId(final String id) {
        return id.substring(id.lastIndexOf('/') + 1);
    }

    private String headersFilename(final String contentFilename) {
        return FCREPO_DIR + contentFilename + ".json";
    }

    private String resolveF6DatastreamId(final String datastreamId, final String f6ObjectId, final String mimeType) {
        var id = f6ObjectId + "/" + datastreamId;

        if (addDatastreamExtensions && !Strings.isNullOrEmpty(mimeType)) {
            id += getExtension(mimeType);
        }

        return id;
    }

    private ResourceHeaders createHeaders(final String id, final String parentId, final String model) {
        final var headers = new ResourceHeaders();
        headers.setId(id);
        headers.setParent(parentId);
        headers.setInteractionModel(model);
        return headers;
    }

    private ResourceHeaders createObjectHeaders(final String f6ObjectId, final ObjectVersionReference ov) {
        final var headers = createHeaders(f6ObjectId, FCREPO_ROOT, BASIC_CONTAINER);
        headers.setArchivalGroup(true);
        headers.setObjectRoot(true);
        headers.setLastModifiedBy(user);
        headers.setCreatedBy(user);

        ov.getObjectProperties().listProperties().forEach(p -> {
            if (p.getName().contains("lastModifiedDate")) {
                headers.setLastModifiedDate(Instant.parse(p.getValue()));
            } else if (p.getName().contains("createdDate")) {
                headers.setCreatedDate(Instant.parse(p.getValue()));
            }
        });

        headers.setStateToken(DigestUtils.md5Hex(
                String.valueOf(headers.getLastModifiedDate().toEpochMilli())).toUpperCase());

        return headers;
    }

    private ResourceHeaders createDatastreamHeaders(final DatastreamVersion dv,
                                         final String f6DsId,
                                         final String f6ObjectId,
                                         final String filename,
                                         final String mime,
                                         final String createDate) {
        final ResourceHeaders headers = createHeaders(f6DsId, f6ObjectId, NON_RDF_SOURCE);
        headers.setFilename(filename);
        headers.setCreatedDate(Instant.parse(createDate));
        headers.setLastModifiedDate(Instant.parse(dv.getCreated()));
        headers.setLastModifiedBy(user);
        headers.setCreatedBy(user);

        if ("RE".contains(dv.getDatastreamInfo().getControlGroup())) {
            headers.setExternalHandling(
                    externalHandlingMap.get(dv.getDatastreamInfo().getControlGroup()));
            headers.setExternalUrl(dv.getExternalOrRedirectURL());
        }

        headers.setArchivalGroup(false);
        headers.setObjectRoot(false);
        headers.setContentSize(dv.getSize());

        if (dv.getContentDigest() != null && !Strings.isNullOrEmpty(dv.getContentDigest().getDigest())) {
            final var digest = dv.getContentDigest();
            headers.setDigests(List.of(URI.create(
                    digest.getType().toLowerCase() + ":" + digest.getDigest().toLowerCase())));
        }

        headers.setMimeType(mime);
        headers.setStateToken(DigestUtils.md5Hex(
                String.valueOf(headers.getLastModifiedDate().toEpochMilli())).toUpperCase());

        return headers;
    }

    private ResourceHeaders createDescriptionHeaders(final String f6DsId,
                                                     final String filename,
                                                     final ResourceHeaders datastreamHeaders) {
        final var id = f6DescriptionId(f6DsId);
        final var headers = createHeaders(id, f6DsId, NON_RDF_SOURCE_DESCRIPTION);

        headers.setFilename(filename);
        headers.setCreatedDate(datastreamHeaders.getCreatedDate());
        headers.setLastModifiedDate(datastreamHeaders.getLastModifiedDate());
        headers.setCreatedBy(datastreamHeaders.getCreatedBy());
        headers.setLastModifiedBy(datastreamHeaders.getLastModifiedBy());

        headers.setArchivalGroup(false);
        headers.setObjectRoot(false);
        headers.setStateToken(datastreamHeaders.getStateToken());

        return headers;
    }

    private String resolveMimeType(final DatastreamVersion dv) {
        String mime = dv.getMimeType();

        if (Strings.isNullOrEmpty(mime)) {
            final var meta = new Metadata();
            meta.set(Metadata.RESOURCE_NAME_KEY, dv.getDatastreamInfo().getDatastreamId());
            try (var content = dv.getContent()) {
                mime = mimeDetector.detect(TikaInputStream.get(content), meta).toString();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        return mime;
    }

    private InputStream serializeHeaders(final ResourceHeaders headers) {
        try {
            return new ByteArrayInputStream(objectMapper.writeValueAsBytes(headers));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
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

        triples.write(out, "N-TRIPLES");
        return new ByteArrayInputStream(out.toByteArray());
    }

    // Get datastream-level triples
    private InputStream getDsTriples(final DatastreamVersion dv,
                                            final String f6DsId,
                                            final String createDate) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Model triples = ModelFactory.createDefaultModel();

        if (migrationType == MigrationType.VANILLA_OCFL) {
            // These triples are server managed in F6
            addDateLiteral(triples,
                    f6DsId,
                    "http://fedora.info/definitions/v4/repository#created",
                    createDate);
            addDateLiteral(triples,
                    f6DsId,
                    "http://fedora.info/definitions/v4/repository#lastModified",
                    dv.getCreated());
            addStringLiteral(triples,
                    f6DsId,
                    "http://purl.org/dc/terms/identifier",
                    dv.getDatastreamInfo().getDatastreamId());
            addStringLiteral(triples,
                    f6DsId,
                    "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#hasMimeType",
                    dv.getMimeType());
            addLongLiteral(triples,
                    f6DsId,
                    "http://www.loc.gov/premis/rdf/v1#size",
                    dv.getSize());

            if (dv.getContentDigest() != null) {
                addStringLiteral(triples,
                        f6DsId,
                        "http://www.loc.gov/premis/rdf/v1#hasMessageDigest",
                        "urn:" + dv.getContentDigest().getType().toLowerCase() + ":" +
                                dv.getContentDigest().getDigest().toLowerCase());
            }
        }

        addStringLiteral(triples,
                         f6DsId,
                         "http://purl.org/dc/terms/title",
                         dv.getLabel());
        addStringLiteral(triples,
                         f6DsId,
                         "http://fedora.info/definitions/1/0/access/objState",
                         dv.getDatastreamInfo().getState());
        addStringLiteral(triples,
                         f6DsId,
                         "http://www.loc.gov/premis/rdf/v1#formatDesignation",
                         dv.getFormatUri());

        triples.write(out, "N-TRIPLES");
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

    private static void addLongLiteral(final Model m,
                                       final String s,
                                       final String p,
                                       final long number) {
        if (number != -1) {
            m.addLiteral(m.createResource(s),
                    m.createProperty(p),
                    m.createTypedLiteral(number, XSDDatatype.XSDlong));
        }
    }

    /**
     * @param mime any mimetype as String
     * @return extension associated with arg mime, return includes '.' in extension (.txt).
     *                  ..Empty String if unrecognized mime
     */
    private static String getExtension(final String mime) {
        final MimeTypes allTypes = MimeTypes.getDefaultMimeTypes();
        MimeType type;
        try {
            type = allTypes.forName(mime);
        } catch (final MimeTypeException e) {
            type = null;
        }

        if (type != null) {
            return type.getExtension();
        }

        LOGGER.warn("No mimetype found for '{}'", mime);
        return "";
    }

}
