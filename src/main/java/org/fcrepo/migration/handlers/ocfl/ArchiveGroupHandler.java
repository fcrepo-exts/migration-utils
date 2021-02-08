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
import org.fcrepo.storage.ocfl.InteractionModel;
import org.fcrepo.storage.ocfl.OcflObjectSession;
import org.fcrepo.storage.ocfl.OcflObjectSessionFactory;
import org.fcrepo.storage.ocfl.ResourceHeaders;
import org.fcrepo.storage.ocfl.ResourceHeadersVersion;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private static final Map<String, String> externalHandlingMap = Map.of(
            "E", "proxy",
            "R", "redirect"
    );

    private static final String INLINE_XML = "X";

    private static final String DS_INACTIVE = "I";
    private static final String DS_DELETED = "D";

    private static final String OBJ_STATE_PROP = "info:fedora/fedora-system:def/model#state";
    private static final String OBJ_INACTIVE = "Inactive";
    private static final String OBJ_DELETED = "Deleted";

    private final OcflObjectSessionFactory sessionFactory;
    private final boolean addDatastreamExtensions;
    private final boolean deleteInactive;
    private final MigrationType migrationType;
    private final String user;
    private final Detector mimeDetector;

    /**
     * Create an ArchiveGroupHandler,
     *
     * @param sessionFactory
     *        OCFL session factory
     * @param migrationType
     *        the type of migration to do
     * @param addDatastreamExtensions
     *        true if datastreams should be written with file extensions
     * @param deleteInactive
     *        true if inactive objects and datastreams should be migrated as deleted
     * @param user
     *        the username to associated with the migrated resources
     */
    public ArchiveGroupHandler(final OcflObjectSessionFactory sessionFactory,
                               final MigrationType migrationType,
                               final boolean addDatastreamExtensions,
                               final boolean deleteInactive,
                               final String user) {
        this.sessionFactory = Preconditions.checkNotNull(sessionFactory, "sessionFactory cannot be null");
        this.migrationType = Preconditions.checkNotNull(migrationType, "migrationType cannot be null");
        this.addDatastreamExtensions = addDatastreamExtensions;
        this.deleteInactive = deleteInactive;
        this.user = Preconditions.checkNotNull(Strings.emptyToNull(user), "user cannot be blank");
        try {
            this.mimeDetector = new TikaConfig().getDetector();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void processObjectVersions(final Iterable<ObjectVersionReference> versions) {
        // We use the PID to identify the OCFL object
        String objectId = null;
        String f6ObjectId = null;

        // We need to manually keep track of the datastream creation dates
        final Map<String, String> dsCreateDates = new HashMap<>();

        String objectState = null;
        final Map<String, String> datastreamStates = new HashMap<>();

        for (var ov : versions) {
            if (ov.isFirstVersion()) {
                objectId = ov.getObjectInfo().getPid();
                f6ObjectId = FCREPO_ROOT + objectId;
                objectState = getObjectState(ov);
            }

            final OcflObjectSession session = sessionFactory.newSession(f6ObjectId);

            // Object properties are written only once (as fcrepo3 object properties were unversioned).
            if (ov.isFirstVersion()) {
                writeObjectFiles(f6ObjectId, ov, session);
            }

            // Write datastreams and their metadata
            for (var dv : ov.listChangedDatastreams()) {
                final var mimeType = resolveMimeType(dv);
                final String dsId = dv.getDatastreamInfo().getDatastreamId();
                final String f6DsId = resolveF6DatastreamId(dsId, f6ObjectId, mimeType);
                final var datastreamFilename = lastPartFromId(f6DsId);

                if (dv.isFirstVersionIn(ov.getObject())) {
                    dsCreateDates.put(dsId, dv.getCreated());
                    datastreamStates.put(f6DsId, dv.getDatastreamInfo().getState());
                }
                final var createDate = dsCreateDates.get(dsId);

                final var datastreamHeaders = createDatastreamHeaders(dv, f6DsId, f6ObjectId,
                        datastreamFilename, mimeType, createDate);

                if (externalHandlingMap.containsKey(dv.getDatastreamInfo().getControlGroup())) {
                    InputStream content = null;
                    // Write a file for external content only for plain OCFL migration
                    if (migrationType == MigrationType.PLAIN_OCFL) {
                        content = IOUtils.toInputStream(dv.getExternalOrRedirectURL());
                    }
                    session.writeResource(datastreamHeaders, content);
                } else {
                    try (var content = dv.getContent()) {
                        session.writeResource(datastreamHeaders, content);
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }

                writeDescriptionFiles(f6DsId, datastreamFilename, createDate, datastreamHeaders, dv, session);
            }

            LOGGER.debug("Committing object <{}>", f6ObjectId);

            session.versionCreationTimestamp(OffsetDateTime.parse(ov.getVersionDate()));
            session.commit();
        }

        handleDeletedResources(f6ObjectId, objectState, datastreamStates);
    }

    private void handleDeletedResources(final String f6ObjectId,
                                        final String objectState,
                                        final Map<String, String> datastreamStates) {
        final OcflObjectSession session = sessionFactory.newSession(f6ObjectId);

        try {
            final var now = OffsetDateTime.now();
            final var hasDeletes = new AtomicBoolean(false);

            if (OBJ_DELETED.equals(objectState) || (deleteInactive && OBJ_INACTIVE.equals(objectState))) {
                hasDeletes.set(true);

                datastreamStates.keySet().forEach(f6DsId -> {
                    deleteDatastream(f6DsId, now.toInstant(), session);
                });

                if (migrationType == MigrationType.PLAIN_OCFL) {
                    deleteOcflMigratedResource(f6ObjectId, InteractionModel.BASIC_CONTAINER, session);
                } else {
                    deleteF6MigratedResource(f6ObjectId, now.toInstant(), session);
                }
            } else {
                datastreamStates.forEach((f6DsId, state) -> {
                    if (DS_DELETED.equals(state) || (deleteInactive && DS_INACTIVE.equals(state))) {
                        hasDeletes.set(true);
                        deleteDatastream(f6DsId, now.toInstant(), session);
                    }
                });
            }

            if (hasDeletes.get()) {
                session.versionCreationTimestamp(now);
                session.commit();
            } else {
                session.abort();
            }
        } catch (RuntimeException e) {
            session.abort();
            throw e;
        }
    }

    private void writeObjectFiles(final String f6ObjectId,
                                  final ObjectVersionReference ov,
                                  final OcflObjectSession session) {
        final var objectHeaders = createObjectHeaders(f6ObjectId, ov);
        final var content = getObjTriples(ov);
        session.writeResource(objectHeaders, content);
    }

    private void writeDescriptionFiles(final String f6Dsid,
                                       final String datastreamFilename,
                                       final String createDate,
                                       final ResourceHeaders datastreamHeaders,
                                       final DatastreamVersion dv,
                                       final OcflObjectSession session) {
        final var descriptionHeaders = createDescriptionHeaders(f6Dsid,
                datastreamFilename,
                datastreamHeaders);
        session.writeResource(descriptionHeaders, getDsTriples(dv, f6Dsid, createDate));
    }

    private String f6DescriptionId(final String f6ResourceId) {
        return f6ResourceId + "/fcr:metadata";
    }

    private String lastPartFromId(final String id) {
        return id.substring(id.lastIndexOf('/') + 1);
    }

    private String resolveF6DatastreamId(final String datastreamId, final String f6ObjectId, final String mimeType) {
        var id = f6ObjectId + "/" + datastreamId;

        if (addDatastreamExtensions && !Strings.isNullOrEmpty(mimeType)) {
            id += getExtension(mimeType);
        }

        return id;
    }

    private ResourceHeaders.Builder createHeaders(final String id,
                                                  final String parentId,
                                                  final InteractionModel model) {
        final var headers = ResourceHeaders.builder();
        headers.withHeadersVersion(ResourceHeadersVersion.V1_0);
        headers.withId(id);
        headers.withParent(parentId);
        headers.withInteractionModel(model.getUri());
        return headers;
    }

    private ResourceHeaders createObjectHeaders(final String f6ObjectId, final ObjectVersionReference ov) {
        final var headers = createHeaders(f6ObjectId, FCREPO_ROOT, InteractionModel.BASIC_CONTAINER);
        headers.withArchivalGroup(true);
        headers.withObjectRoot(true);
        headers.withLastModifiedBy(user);
        headers.withCreatedBy(user);

        ov.getObjectProperties().listProperties().forEach(p -> {
            if (p.getName().contains("lastModifiedDate")) {
                final var lastModified = Instant.parse(p.getValue());
                headers.withLastModifiedDate(lastModified);
                headers.withMementoCreatedDate(lastModified);
                headers.withStateToken(DigestUtils.md5Hex(
                        String.valueOf(lastModified.toEpochMilli())).toUpperCase());
            } else if (p.getName().contains("createdDate")) {
                headers.withCreatedDate(Instant.parse(p.getValue()));
            }
        });

        return headers.build();
    }

    private ResourceHeaders createDatastreamHeaders(final DatastreamVersion dv,
                                                    final String f6DsId,
                                                    final String f6ObjectId,
                                                    final String filename,
                                                    final String mime,
                                                    final String createDate) {
        final var lastModified = Instant.parse(dv.getCreated());
        final var headers = createHeaders(f6DsId, f6ObjectId, InteractionModel.NON_RDF);
        headers.withArchivalGroupId(f6ObjectId);
        headers.withFilename(filename);
        headers.withCreatedDate(Instant.parse(createDate));
        headers.withLastModifiedDate(lastModified);
        headers.withLastModifiedBy(user);
        headers.withCreatedBy(user);
        headers.withMementoCreatedDate(lastModified);

        if (externalHandlingMap.containsKey(dv.getDatastreamInfo().getControlGroup())) {
            headers.withExternalHandling(
                    externalHandlingMap.get(dv.getDatastreamInfo().getControlGroup()));
            headers.withExternalUrl(dv.getExternalOrRedirectURL());
        }

        headers.withArchivalGroup(false);
        headers.withObjectRoot(false);
        if (dv.getSize() > -1 && !INLINE_XML.equals(dv.getDatastreamInfo().getControlGroup())) {
            headers.withContentSize(dv.getSize());
        }

        if (dv.getContentDigest() != null && !Strings.isNullOrEmpty(dv.getContentDigest().getDigest())) {
            final var digest = dv.getContentDigest();
            final var digests = new ArrayList<URI>();
            digests.add(URI.create("urn:" + digest.getType().toLowerCase() + ":" + digest.getDigest().toLowerCase()));
            headers.withDigests(digests);
        }

        headers.withMimeType(mime);
        headers.withStateToken(DigestUtils.md5Hex(
                String.valueOf(lastModified.toEpochMilli())).toUpperCase());

        return headers.build();
    }

    private ResourceHeaders createDescriptionHeaders(final String f6DsId,
                                                     final String filename,
                                                     final ResourceHeaders datastreamHeaders) {
        final var id = f6DescriptionId(f6DsId);
        final var headers = createHeaders(id, f6DsId, InteractionModel.NON_RDF_DESCRIPTION);

        headers.withArchivalGroupId(datastreamHeaders.getArchivalGroupId());
        headers.withFilename(filename);
        headers.withCreatedDate(datastreamHeaders.getCreatedDate());
        headers.withLastModifiedDate(datastreamHeaders.getLastModifiedDate());
        headers.withCreatedBy(datastreamHeaders.getCreatedBy());
        headers.withLastModifiedBy(datastreamHeaders.getLastModifiedBy());
        headers.withMementoCreatedDate(datastreamHeaders.getMementoCreatedDate());

        headers.withArchivalGroup(false);
        headers.withObjectRoot(false);
        headers.withStateToken(datastreamHeaders.getStateToken());

        return headers.build();
    }

    private String resolveMimeType(final DatastreamVersion dv) {
        String mime = dv.getMimeType();

        if (Strings.isNullOrEmpty(mime)) {
            final var meta = new Metadata();
            meta.set(Metadata.RESOURCE_NAME_KEY, dv.getDatastreamInfo().getDatastreamId());
            try (var content = TikaInputStream.get(dv.getContent())) {
                mime = mimeDetector.detect(content, meta).toString();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        return mime;
    }

    private void deleteDatastream(final String id,
                                  final Instant lastModified,
                                  final OcflObjectSession session) {
        if (migrationType == MigrationType.PLAIN_OCFL) {
            deleteOcflMigratedResource(id, InteractionModel.NON_RDF, session);
            deleteOcflMigratedResource(f6DescriptionId(id), InteractionModel.NON_RDF_DESCRIPTION, session);
        } else {
            deleteF6MigratedResource(id, lastModified, session);
            deleteF6MigratedResource(f6DescriptionId(id), lastModified, session);
        }
    }

    private void deleteF6MigratedResource(final String id,
                                          final Instant lastModified,
                                          final OcflObjectSession session) {
        LOGGER.debug("Deleting resource {}", id);
        final var headers = session.readHeaders(id);
        session.deleteContentFile(ResourceHeaders.builder(headers)
                .withDeleted(true)
                .withLastModifiedDate(lastModified)
                .withMementoCreatedDate(lastModified)
                .build());
    }

    private void deleteOcflMigratedResource(final String id,
                                            final InteractionModel interactionModel,
                                            final OcflObjectSession session) {
        LOGGER.debug("Deleting resource {}", id);
        session.deleteContentFile(ResourceHeaders.builder()
                .withId(id)
                .withInteractionModel(interactionModel.getUri())
                .build());
    }

    private String getObjectState(final ObjectVersionReference ov) {
        return ov.getObjectProperties().listProperties().stream()
                .filter(prop -> OBJ_STATE_PROP.equals(prop.getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(String.format("Object %s is missing state information",
                        ov.getObjectInfo().getPid())))
                .getValue();
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

        if (migrationType == MigrationType.PLAIN_OCFL) {
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
