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

import at.favre.lib.bytes.Bytes;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;
import org.fcrepo.migration.ContentDigest;
import org.fcrepo.migration.DatastreamVersion;
import org.fcrepo.migration.FedoraObjectVersionHandler;
import org.fcrepo.migration.MigrationType;
import org.fcrepo.migration.ObjectInfo;
import org.fcrepo.migration.ObjectVersionReference;
import org.fcrepo.storage.ocfl.InteractionModel;
import org.fcrepo.storage.ocfl.OcflObjectSession;
import org.fcrepo.storage.ocfl.OcflObjectSessionFactory;
import org.fcrepo.storage.ocfl.ResourceHeaders;
import org.fcrepo.storage.ocfl.ResourceHeadersVersion;
import org.fcrepo.storage.ocfl.exception.NotFoundException;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
    private static final String DOWNLOAD_NAME_PROP = "info:fedora/fedora-system:def/model#downloadFilename";
    private static final String OBJ_INACTIVE = "Inactive";
    private static final String OBJ_DELETED = "Deleted";

    private static final String RELS_EXT = "RELS-EXT";
    private static final String RELS_INT = "RELS-INT";

    private final OcflObjectSessionFactory sessionFactory;
    private final boolean addDatastreamExtensions;
    private final boolean deleteInactive;
    private final boolean foxmlFile;
    private final MigrationType migrationType;
    private final String user;
    private final String idPrefix;
    private final Detector mimeDetector;
    private final boolean disableChecksumValidation;

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
     * @param foxmlFile
     *        true if foxml file should be migrated as a whole file, instead of creating property files
     * @param user
     *        the username to associated with the migrated resources
     * @param idPrefix
     *        the prefix to add to the Fedora 3 pid (default "info:fedora/", like Fedora 3)
     * @param disableChecksumValidation
     *        if true, migrator should not try to verify that the datastream content matches Fedora 3 checksums
     */
    public ArchiveGroupHandler(final OcflObjectSessionFactory sessionFactory,
                               final MigrationType migrationType,
                               final boolean addDatastreamExtensions,
                               final boolean deleteInactive,
                               final boolean foxmlFile,
                               final String user,
                               final String idPrefix,
                               final boolean disableChecksumValidation) {
        this.sessionFactory = Preconditions.checkNotNull(sessionFactory, "sessionFactory cannot be null");
        this.migrationType = Preconditions.checkNotNull(migrationType, "migrationType cannot be null");
        this.addDatastreamExtensions = addDatastreamExtensions;
        this.deleteInactive = deleteInactive;
        this.foxmlFile = foxmlFile;
        this.user = Preconditions.checkNotNull(Strings.emptyToNull(user), "user cannot be blank");
        this.idPrefix = idPrefix;
        this.disableChecksumValidation = disableChecksumValidation;
        try {
            this.mimeDetector = new TikaConfig().getDetector();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void processObjectVersions(final Iterable<ObjectVersionReference> versions, final ObjectInfo objectInfo) {
        // We use the PID to identify the OCFL object
        final String objectId = objectInfo.getPid();
        final String f6ObjectId = idPrefix + objectId;

        // We need to manually keep track of the datastream creation dates
        final Map<String, String> dsCreateDates = new HashMap<>();

        String objectState = null;
        final Map<String, String> datastreamStates = new HashMap<>();
        final Map<String, MetaHolder> metaMap = new HashMap<>();

        for (var ov : versions) {
            final Set<String> toWrite = new HashSet<>();
            final Map<String, String> filenameChanges = new HashMap<>();

            final OcflObjectSession session = new OcflObjectSessionWrapper(sessionFactory.newSession(f6ObjectId));

            if (ov.isFirstVersion()) {
                if (session.containsResource(f6ObjectId)) {
                    throw new RuntimeException(f6ObjectId + " already exists!");
                }
                objectState = getObjectState(ov, objectId);
                // Object properties are written only once (as fcrepo3 object properties were unversioned).
                if (foxmlFile) {
                    try (InputStream is = new BufferedInputStream(Files.newInputStream(objectInfo.getFoxmlPath()))) {
                        final var foxmlDsId = f6ObjectId + "/FOXML";
                        final var headers = createHeaders(foxmlDsId, f6ObjectId,
                                InteractionModel.NON_RDF).build();
                        session.writeResource(headers, is);
                        //mark FOXML as a deleted datastream so it gets deleted in handleDeletedResources()
                        datastreamStates.put(foxmlDsId, DS_DELETED);
                    } catch (IOException io) {
                        LOGGER.error("error writing " + objectId + " FOXML file to " + f6ObjectId + ": " + io);
                        throw new UncheckedIOException(io);
                    }
                } else {
                    final var objectHeaders = createObjectHeaders(f6ObjectId, ov);
                    final var content = getObjTriples(ov, objectId);
                    final var meta = MetaHolder.fromContent(content, objectHeaders);
                    metaMap.put(f6ObjectId, meta);
                    session.writeResource(meta.headers.build(), meta.constructTriples());
                }
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
                    // for plain OCFL migrations, write a file containing the external/redirect URL
                    if (migrationType == MigrationType.PLAIN_OCFL) {
                        content = IOUtils.toInputStream(dv.getExternalOrRedirectURL(), StandardCharsets.UTF_8);
                    }
                    session.writeResource(datastreamHeaders, content);
                } else {
                    try (var contentStream = dv.getContent()) {
                        writeDatastreamContent(dv, datastreamHeaders, contentStream, session);
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }

                if (!foxmlFile) {
                    final var f6DescId = f6DescriptionId(f6DsId);
                    final var descriptionHeaders = createDescriptionHeaders(f6DsId,
                            datastreamFilename,
                            datastreamHeaders);
                    final var descriptionTriples = getDsTriples(dv, f6DsId, createDate);
                    metaMap.computeIfAbsent(f6DescId, k -> new MetaHolder())
                            .setHeaders(descriptionHeaders)
                            .setContentTriples(descriptionTriples);
                    toWrite.add(f6DescId);

                    if (RELS_EXT.equals(dsId) || RELS_INT.equals(dsId)) {
                        final var triples = parseRdfXml(dv);
                        if (RELS_EXT.equals(dsId)) {
                            metaMap.get(f6ObjectId).setRelsTriples(triples);
                            toWrite.add(f6ObjectId);
                        } else {
                            final Map<String, Model> splitModels = splitRelsInt(triples);
                            splitModels.forEach((id, model) -> {
                                final var descId = f6DescriptionId(id);
                                metaMap.computeIfAbsent(descId, k -> new MetaHolder())
                                        .setRelsTriples(model);
                                toWrite.add(descId);

                                // Check to see if there are any file names that need updated
                                for (final var it = model.listStatements(); it.hasNext(); ) {
                                    final var statement = it.next();
                                    if (DOWNLOAD_NAME_PROP.equals(statement.getPredicate().getURI())) {
                                        filenameChanges.put(id, statement.getObject().toString());
                                        break;
                                    }
                                }
                            });
                        }
                    }
                }
            }

            writeMeta(toWrite, metaMap, session);
            updateFilenames(filenameChanges, session);

            LOGGER.debug("Committing object <{}>", f6ObjectId);

            session.versionCreationTimestamp(OffsetDateTime.parse(ov.getVersionDate()));
            session.commit();
        }

        handleDeletedResources(f6ObjectId, objectState, datastreamStates);
    }

    /**
     * RDF resources are written after writing all other binaries in the version because they can be affected by
     * RELS-INT or RELS-EXT updates.
     *
     * @param toWrite the set of resources that should be written to this version
     * @param metaMap the map of all known rdf resources
     * @param session the current ocfl session
     */
    private void writeMeta(final Set<String> toWrite,
                           final Map<String, MetaHolder> metaMap,
                           final OcflObjectSession session) {
        for (final var id : toWrite) {
            final var meta = metaMap.get(id);

            if (meta.headers == null) {
                // This only happens if there's a RELS-INT that references a datastream before it exists.
                // Skip for now. The triples will be added once the datastream exists.
                continue;
            }

            // Need to copy over the memento created date from the existing headers because it may have been updated
            // when a description's binary was updated
            if (migrationType == MigrationType.FEDORA_OCFL) {
                try {
                    final var existingHeaders = session.readHeaders(id);
                    meta.headers.withMementoCreatedDate(existingHeaders.getMementoCreatedDate());
                } catch (NotFoundException e) {
                    // this just means the resource hasn't been written yet
                }
            }
            session.writeResource(meta.headers.build(), meta.constructTriples());
        }
    }

    private void updateFilenames(final Map<String, String> filenameChanges, final OcflObjectSession session) {
        if (migrationType == MigrationType.FEDORA_OCFL) {
            filenameChanges.forEach((id, filename) -> {
                final var origHeaders = session.readHeaders(id);
                final var newHeaders = ResourceHeaders.builder(origHeaders).withFilename(filename).build();
                session.writeHeaders(newHeaders);
            });
        }
    }

    private boolean fedora3DigestValid(final ContentDigest f3Digest) {
        return f3Digest != null && StringUtils.isNotBlank(f3Digest.getType()) &&
                StringUtils.isNotBlank(f3Digest.getDigest());
    }

    private void writeDatastreamContent(final DatastreamVersion dv,
                                        final ResourceHeaders datastreamHeaders,
                                        final InputStream contentStream,
                                        final OcflObjectSession session) throws IOException {
        if (disableChecksumValidation) {
            session.writeResource(datastreamHeaders, contentStream);
            return;
        }
        final var f3Digest = dv.getContentDigest();
        final var ocflObjectId = session.ocflObjectId();
        final var datastreamId = dv.getDatastreamInfo().getDatastreamId();
        final var datastreamControlGroup = dv.getDatastreamInfo().getControlGroup();
        if (fedora3DigestValid(f3Digest)) {
            try {
                final var messageDigest = MessageDigest.getInstance(f3Digest.getType());
                if (migrationType == MigrationType.PLAIN_OCFL) {
                    session.writeResource(datastreamHeaders, contentStream);
                } else {
                    try (var digestStream = new DigestInputStream(contentStream, messageDigest)) {
                        session.writeResource(datastreamHeaders, digestStream);
                        final var expectedDigest = f3Digest.getDigest();
                        final var actualDigest = Bytes.wrap(digestStream.getMessageDigest().digest()).encodeHex();
                        if (!actualDigest.equalsIgnoreCase(expectedDigest)) {
                            final var msg = String.format("%s/%s: digest %s doesn't match expected digest %s",
                                    ocflObjectId, datastreamId, actualDigest, expectedDigest);
                            throw new RuntimeException(msg);
                        }
                    }
                }
            } catch (final NoSuchAlgorithmException e) {
                final var msg = String.format("%s/%s: no digest algorithm %s. Writing resource & continuing.",
                        ocflObjectId, datastreamId, f3Digest.getType());
                LOGGER.warn(msg);
                session.writeResource(datastreamHeaders, contentStream);
            }
        } else {
            if (datastreamControlGroup.equalsIgnoreCase("M")) {
                final var msg = String.format("%s/%s: missing/invalid digest. Writing resource & continuing.",
                        ocflObjectId, datastreamId);
                LOGGER.warn(msg);
            }
            session.writeResource(datastreamHeaders, contentStream);
        }
    }

    private void handleDeletedResources(final String f6ObjectId,
                                        final String objectState,
                                        final Map<String, String> datastreamStates) {
        final OcflObjectSession session = new OcflObjectSessionWrapper(sessionFactory.newSession(f6ObjectId));

        try {
            final var now = OffsetDateTime.now().withOffsetSameInstant(ZoneOffset.UTC);
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

    private ResourceHeaders.Builder createObjectHeaders(final String f6ObjectId, final ObjectVersionReference ov) {
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

        return headers;
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

    private ResourceHeaders.Builder createDescriptionHeaders(final String f6DsId,
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

        return headers;
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

    private String getObjectState(final ObjectVersionReference ov, final String pid) {
        return ov.getObjectProperties().listProperties().stream()
                .filter(prop -> OBJ_STATE_PROP.equals(prop.getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(String.format("Object %s is missing state information",
                        pid)))
                .getValue();
    }

    // Get object-level triples
    private static Model getObjTriples(final ObjectVersionReference o, final String pid) {
        final Model triples = ModelFactory.createDefaultModel();
        final String uri = "info:fedora/" + pid;

        o.getObjectProperties().listProperties().forEach(p -> {
            if (p.getName().contains("Date")) {
                addDateLiteral(triples, uri, p.getName(), p.getValue());
            } else {
                addStringLiteral(triples, uri, p.getName(), p.getValue());
            }
        });

        return triples;
    }

    // Get datastream-level triples
    private Model getDsTriples(final DatastreamVersion dv,
                                            final String f6DsId,
                                            final String createDate) {
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

        return triples;
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

    private Model parseRdfXml(final DatastreamVersion datastreamVersion) {
        final var model = ModelFactory.createDefaultModel();
        try (final var is = datastreamVersion.getContent()) {
            RDFDataMgr.read(model, is, Lang.RDFXML);
            return model;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to parse RDF XML in %s/%s",
                    datastreamVersion.getDatastreamInfo().getObjectInfo().getPid(),
                    datastreamVersion.getDatastreamInfo().getDatastreamId()), e);
        }
    }

    private Map<String, Model> splitRelsInt(final Model relsIntModel) {
        final Map<String, Model> splitModels = new HashMap<>();
        for (final var it = relsIntModel.listStatements(); it.hasNext();) {
            final var statement = it.next();
            final var id = statement.getSubject().getURI();
            final var model = splitModels.computeIfAbsent(id, k -> ModelFactory.createDefaultModel());
            model.add(statement);
        }
        return splitModels;
    }

    /**
     * Wrapper class for storing a RDF resource's "content" triples, RELS triples, and resource headers. The content
     * triples are triples that were generated based on general Fedora metadata, and the RELS triples are extracted from
     * one of the RELS-* files. They are maintained separately because it's possible for them to be updated
     * independently and we need to be able to construct the correct set of triples when one changes.
     */
    private static class MetaHolder {
        Model contentTriples;
        Model relsTriples;
        ResourceHeaders.Builder headers;

        public static MetaHolder fromContent(final Model contentTriples, final ResourceHeaders.Builder headers) {
            return new MetaHolder(contentTriples, null, headers);
        }

        private MetaHolder() {
        }

        private MetaHolder(final Model contentTriples,
                           final Model relsTriples,
                           final ResourceHeaders.Builder headers) {
            this.contentTriples = contentTriples;
            this.relsTriples = relsTriples;
            this.headers = headers;
        }

        /**
         * Constructs a complete set of triples at the current version of the resource and serializes them as n-triples.
         *
         * @return n-triples input stream
         */
        public InputStream constructTriples() {
            final var output = new ByteArrayOutputStream();
            final var triples = ModelFactory.createDefaultModel();

            if (contentTriples != null) {
                triples.add(contentTriples.listStatements());
            }

            if (relsTriples != null) {
                triples.add(relsTriples.listStatements());
            }

            triples.write(output, Lang.NTRIPLES.getName());
            return new ByteArrayInputStream(output.toByteArray());
        }

        public MetaHolder setHeaders(final ResourceHeaders.Builder headers) {
            this.headers = headers;
            return this;
        }

        public MetaHolder setContentTriples(final Model contentTriples) {
            this.contentTriples = contentTriples;
            return this;
        }

        public MetaHolder setRelsTriples(final Model relsTriples) {
            this.relsTriples = relsTriples;
            return this;
        }
    }

}
