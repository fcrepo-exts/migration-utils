# Migration Utilities [![Build Status](https://travis-ci.org/fcrepo4-exts/migration-utils.png?branch=master)](https://travis-ci.org/fcrepo4-exts/migration-utils)

A framework to support migration of data from Fedora 3 to Fedora 4 repositories.

## Overview

The main class (`org.fcrepo.migration.Migrator`) iterates over all of the fedora objects in a configured source (`org.fcrepo.migration.ObjectSource`) and handles them using the configured handler (`org.fcrepo.migration.StreamingFedoraObjectHandler`). The configuration is entirely contained within a Spring XML configuration file in [`src/main/resources/spring/migration-bean.xml`](https://github.com/fcrepo4-exts/migration-utils/blob/master/src/main/resources/spring/migration-bean.xml).

## Status

A basic migration scenario is implemented that may serve as a starting point for
your own migration from Fedora 3.x to Fedora 4.x.

## How to use

Background work

* Determine the disposition of your FOXML files:
  * Will you be migrating from exported (archive or migration context) FOXML?
    * If so, you will need all of the export FOXML in a known directory.
  * Will you be migrating from from a native fcrepo3 filesystem?
    * If so, fcrepo3 should not be running, and you will need to determine if you're using legacy or akubra storage
* Determine your fcrepo4 url (ex: http://localhost:8080/rest/, http://yourHostName.ca:8080/fcrepo/rest/)

_It is strongly recommended that you set up a local, empty Fedora 4 repository for testing migration of your resources because you can easily throw away the repository (or simply delete its data directory) when you're done testing, or if you wish to test a different migration configuration.  One should wait until one is fully content with the representation of one's migrated content in Fedora 4 before migrating content into an active or production repository._  

Getting started:

* [Download](https://github.com/fcrepo4-exts/migration-utils/releases) and extract the distribution zip file
* Choose an example configuration file that best suits your needs
  * [conf/fedora2-native.xml](https://github.com/fcrepo4-exts/migration-utils/blob/master/conf/fedora2-native.xml) is a good start for migrating from fedora 2
  * [conf/fedora3-akubra.xml](https://github.com/fcrepo4-exts/migration-utils/blob/master/conf/fedora3-akubra.xml) is a good start for migrating from fedora 3 with access to the stored FOXML (akubra FS)
  * [conf/fedora3-legacy.xml](https://github.com/fcrepo4-exts/migration-utils/blob/master/conf/fedora3-legacy.xml) is a good start for migrating from fedora 3 with access to the stored FOXML (legacy FS)
  * [conf/fedora3-exported.xml](https://github.com/fcrepo4-exts/migration-utils/blob/master/conf/fedora3-exported.xml) is a good start for migrating exported FOXML from fedora 3
* Make necessary changes to the configuration to reflect your needs and local set up
  * Most importantly you'll want to set the appropriate fedora 4 URL to which you want to migrate the resources
  * Unless you just want to migrate the included test set, you'll also want to point the configuration to your fedora3 FOXML data files

To run the migration scenario you have configured in the Spring XML configuration file:

```
java -jar migration-utils-{version}-driver.jar <relative-or-absolute-path-to-configuration-file>
```

## Property Mappings

### fcrepo3 Object properties to fcrepo4

| fcrepo 3         | fcrepo4                             | Example                  |
|------------------|-------------------------------------|--------------------------|
| PID              | fedora3model:PID†                   | yul:328697               |
| state            | fedoraaccess:objState               | Active                   |
| label            | fedora3model:label†                 | Elvis Presley            |
| createDate       | premis:hasDateCreatedByApplication  | 2015-03-16T20:11:06.683Z |
| lastModifiedDate | metadataModification                | 2015-03-16T20:11:06.683Z |
| ownerId          | fedora3model:ownerId†               | nruest                   |

### fcrepo3 Datastream properties to fcrepo4

| fcrepo3       | fcrepo4                                                      | Example                                                    |
|---------------|--------------------------------------------------------------|------------------------------------------------------------|
| DSID          | dcterms:identifier                                           | OBJ                                                        |
| Label         | dcterms:title‡                                               | ASC19109.tif                                               |
| MIME Type     | ebucore:hasMimeType†                                         | image/tiff                                                 |
| State         | fedoraaccess:objState                                        | Active                                                     |
| Created       | premis:hasDateCreatedByApplication                           | 2015-03-16T20:11:06.683Z                                   |
| Versionable   | fedora:hasVersions‡                                          | true                                                       |
| Format URI    | premis:formatDesignation‡                                    | info:pronom/fmt/156                                        |
| Alternate IDs | dcterms:identifier‡                                          |                                                            |
| Access URL    | dcterms:identifier‡                                          |                                                            |
| Checksum      | cryptofunc:_hashalgorithm_‡                                  | cryptofunc:sha1 "c91342b705b15cb4f6ac5362cc6a47d9425aec86" |

### auditTrail mapping

| fcrepo3 event                      | fcrepo4 Event Type                              |
|------------------------------------|-------------------------------------------------|
| addDatastream                      | premis:ing‡                                     |
| modifyDatastreamByReference        | audit:contentModification/metadataModification‡ |
| modifyObject                       | audit:resourceModification‡                     |
| modifyObject (checksum validation) | premis:validation‡                              |
| modifyDatastreamByValue            | audit:contentModification/metadataModification‡ |
| purgeDatastream                    | audit:contentRemoval‡                           |

† The `fedora3model` namespace is not a published namespace. It is a representation of the fcrepo3 namespace `info:fedora/fedora-system:def/model`.

‡ Not yet implemented

**Note**: All fcrepo3 DC (Dublin Core) datastream values are mapped as dcterms properties on the Object in fcrepo4. The same goes for any properties in the RELS-EXT and RELS-INT datastreams.

## Additional Documentation

 * [wiki](https://wiki.duraspace.org/display/FF/Fedora+3+to+4+Data+Migration)

## Maintainers

Current maintainers

* [Mike Durbin](https://github.com/mikedurbin)
* [Nick Ruest](https://github.com/ruebot)

