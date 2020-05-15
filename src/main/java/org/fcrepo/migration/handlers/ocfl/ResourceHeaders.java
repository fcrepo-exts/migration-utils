/*
 * Licensed to DuraSpace under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * DuraSpace licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
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

import java.net.URI;
import java.time.Instant;
import java.util.Collection;

/**
 * Resource headers contain metadata about Fedora resources in Fedora 6.
 * <p>
 * This class is based on ResourceHeadersImpl in fcrepo-persistence-common.
 *
 * @author bbpennel
 * @author pwinckles
 */
public class ResourceHeaders {

    private String id;

    private String parent;

    private String stateToken;

    private String interactionModel;

    private String mimeType;

    private String filename;

    private Long contentSize;

    private Collection<URI> digests;

    private String externalUrl;

    private String externalHandling;

    private Instant createdDate;

    private String createdBy;

    private Instant lastModifiedDate;

    private String lastModifiedBy;

    private boolean archivalGroup;

    private boolean objectRoot;

    private boolean deleted;

    /**
     * @return the fedora id
     */
    public String getId() {
        return id;
    }

    /**
     * @param id the fedora id to set
     */
    public void setId(final String id) {
        this.id = id;
    }

    /**
     * @return the parent fedora id
     */
    public String getParent() {
        return parent;
    }

    /**
     * @param parent the parent to set
     */
    public void setParent(final String parent) {
        this.parent = parent;
    }

    /**
     * @return the stateToken
     */
    public String getStateToken() {
        return stateToken;
    }

    /**
     * @param stateToken the stateToken to set
     */
    public void setStateToken(final String stateToken) {
        this.stateToken = stateToken;
    }

    /**
     * @return the interactionModel
     */
    public String getInteractionModel() {
        return interactionModel;
    }

    /**
     * @param interactionModel the interactionModel to set
     */
    public void setInteractionModel(final String interactionModel) {
        this.interactionModel = interactionModel;
    }

    /**
     * @return the mimeType
     */
    public String getMimeType() {
        return mimeType;
    }

    /**
     * @param mimeType the mimeType to set
     */
    public void setMimeType(final String mimeType) {
        this.mimeType = mimeType;
    }

    /**
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * @param filename the filename to set
     */
    public void setFilename(final String filename) {
        this.filename = filename;
    }

    /**
     * @return the contentSize
     */
    public Long getContentSize() {
        return contentSize;
    }

    /**
     * @param contentSize the contentSize to set
     */
    public void setContentSize(final Long contentSize) {
        this.contentSize = contentSize;
    }

    /**
     * @return the digests
     */
    public Collection<URI> getDigests() {
        return digests;
    }

    /**
     * @param digests the digests to set
     */
    public void setDigests(final Collection<URI> digests) {
        this.digests = digests;
    }

    /**
     * @return the externalHandling
     */
    public String getExternalHandling() {
        return externalHandling;
    }

    /**
     * @param externalHandling the externalHandling to set
     */
    public void setExternalHandling(final String externalHandling) {
        this.externalHandling = externalHandling;
    }

    /**
     * @return the createdDate
     */
    public Instant getCreatedDate() {
        return createdDate;
    }

    /**
     * @param createdDate the createdDate to set
     */
    public void setCreatedDate(final Instant createdDate) {
        this.createdDate = createdDate;
    }

    /**
     * @return the createdBy
     */
    public String getCreatedBy() {
        return createdBy;
    }

    /**
     * @param createdBy the createdBy to set
     */
    public void setCreatedBy(final String createdBy) {
        this.createdBy = createdBy;
    }

    /**
     * @return the lastModifiedDate
     */
    public Instant getLastModifiedDate() {
        return lastModifiedDate;
    }

    /**
     * @param lastModifiedDate the lastModifiedDate to set
     */
    public void setLastModifiedDate(final Instant lastModifiedDate) {
        this.lastModifiedDate = lastModifiedDate;
    }

    /**
     * @return the lastModifiedBy
     */
    public String getLastModifiedBy() {
        return lastModifiedBy;
    }

    /**
     * @param lastModifiedby the lastModifiedby to set
     */
    public void setLastModifiedBy(final String lastModifiedby) {
        this.lastModifiedBy = lastModifiedby;
    }

    /**
     * @param externalUrl the externalUrl to set
     */
    public void setExternalUrl(final String externalUrl) {
        this.externalUrl = externalUrl;
    }

    /**
     * @return the externalUrl
     */
    public String getExternalUrl() {
        return externalUrl;
    }

    /**
     *
     * @param flag boolean flag
     */
    public void setArchivalGroup(final boolean flag) {
        this.archivalGroup = flag;
    }

    /**
     * @return if it is an archival group
     */
    public boolean isArchivalGroup() {
        return archivalGroup;
    }

    /**
     * @param flag boolean flag
     */
    public void setObjectRoot(final boolean flag) {
        this.objectRoot = flag;
    }

    /**
     * @return if it is an object root resource
     */
    public boolean isObjectRoot() {
        if (isArchivalGroup()) {
            return true;
        } else {
            return objectRoot;
        }
    }

    /**
     * Set deleted status flag.
     * @param deleted true if deleted (a tombstone).
     */
    public void setDeleted(final boolean deleted) {
        this.deleted = deleted;
    }

    /**
     * @return if the resource is deleted
     */
    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public String toString() {
        return "ResourceHeaders{" +
                "id='" + id + '\'' +
                ", parent='" + parent + '\'' +
                ", stateToken='" + stateToken + '\'' +
                ", interactionModel='" + interactionModel + '\'' +
                ", mimeType='" + mimeType + '\'' +
                ", filename='" + filename + '\'' +
                ", contentSize=" + contentSize +
                ", digests=" + digests +
                ", externalUrl='" + externalUrl + '\'' +
                ", externalHandling='" + externalHandling + '\'' +
                ", createdDate=" + createdDate +
                ", createdBy='" + createdBy + '\'' +
                ", lastModifiedDate=" + lastModifiedDate +
                ", lastModifiedBy='" + lastModifiedBy + '\'' +
                ", archivalGroup=" + archivalGroup +
                ", objectRoot=" + objectRoot +
                ", deleted=" + deleted +
                '}';
    }

}
