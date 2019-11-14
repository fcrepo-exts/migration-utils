# Migration Utilities [![Build Status](https://travis-ci.org/fcrepo4-exts/migration-utils.png?branch=master)](https://travis-ci.org/fcrepo4-exts/migration-utils)

A framework to support migration of data from Fedora 3 to Fedora 4, 5, or 6 repositories

## Overview

This utility iterates the foxml files of a fedora 2 or 3 repository, and populates a fedora 4, 5, or 6 repository.

For migrations to Fedora 4 and 5, the utility populates the repository via its APIs.  You will need a running fedora 4 or fedora 5 repository to perform the migration.
The utility will perform various mapping operations in order to fit the fedora 2/3 model onto LDP as supported in Fedora 4 and 5.  In particular:

* All RDF URIs will be re-mapped.  Fedora 2 and 3 use `info:fedora/` URIs in `RELS-EXT` and `RELS-INT`.  The migration utility will re-write these URIs into resolvable `http://` URIs that point to the corresponding resources in the fedora 4 or 5 repository
* FOXML object properties will be expressed in terms of RDF according to the mapping defined in `${migration.mapping.file}`. See example [custom-mapping.properties](https://github.com/fcrepo4-exts/migration-utils/blob/master/src/main/resources/custom-mapping.properties).
* TODO: is there more?

Migrations to Fedora 6 may take a different approach, writing migrated objects directly to the filesystem as [OCFL](https://ocfl.io/draft/spec/)
objects.  Additionally, a "minimal" migration mode is available that performs fewer transformations to migrated content.
In particular:

* There is a 1:1 correspondence between fedora 3 objects and OCFL objects.  Fedora 3 datastreams appear as files within the resulting OCFL objects.
* RDF is not re-mapped, `info:fedora/` subjects and objects are kept intact as-is
* FOXML object and datastream properties are represented as triples in additional sidecar files as per the mapping defined in `${migration.mapping.file}`. See example [custom-mapping.properties](https://github.com/fcrepo4-exts/migration-utils/blob/master/src/main/resources/custom-mapping.properties).

## Status

Fedora 6 support is being actively developed, and is considered unstable until Fedora 6 is released.

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

*Warning*: _The migration tool is under active development, so these instructions will change as the configuration process becomes more refined_  

General usage of the migration utils CLI is as follows:

```java [system properties] -jar target/migration-utils-4.4.1-SNAPSHOT-driver.jar conf/fedora3.xml```

The system properties determine the specific details of the migration, and are defined as follows:

* `migration.layout` {exported, legacy, akubra}.  Foxml and datastream layout.  Default ***`exported`***
* `migration.limit` Integer.  Maximum number of objects to export.  Default ***`2`***
* `migration.strategy` {ldpFull, minimal}.  Migration strategy/technique from mapping Fedora 3 to LDP or OCFL.  `ldpFull` is the only option suitable for fcrepo4 and fcrepo5 migrations, as it performs the full suite of transformations from the fedora 3 model onto LDP.  `minimal` is an option for Fedora 6 for a transparent 1:1 mapping between fedora 3 objects and OCFL objects.  Default ***ldpFull***
* `migration.import.external` {false, true}.  Whether to migrate external content.  Default ***`false`***
* `migration.import.redirect` {false, true}.  Whether to migrate redirected content.  Default ***`false`***
* `migration.mapping.file`.  File that has RDF predicate mappings in it for transforming migrated triples.  Default ***`src/test/resources/custom-mapping.properties`***
* `migration.namespace.file`.  RDF namespace file.  Default  ***`src/main/resources/namespaces.properties`***
* `migration.ocfl.storage.dir`.  Path to OCFL storage dir.  Only relevant when `fedora.client` is `ocfl`.  Default ***`target/test/ocfl`***
* `migration.ocfl.staging.dir`.  Path to OCFL staging dir.  Only relevant when `fedora.client` is `ocfl`. Default ***`target/test/staging`***
* `migration.ocfl.layout`.  Storage layout approach. Options include FLAT, PAIRTREE, and TRUNCATED. Only relevant when `fedora.client` is `ocfl`. Default ***`FLAT`***
* `migration.pid.list.file`.  File containing list of PIDs to migrate.  Only relevant when `fedora.client` is `ocfl`. Default ***`null`***
* `migration.pid.resume.dir`.  Path to directory in which a "resume file" will be created/used in the case that a previous run must be resumed from the last exported PID/Object.  Only relevant when `fedora.client` is `ocfl`. Default ***`target/test/pid`***
* `migration.pid.resume.all`.  Boolean flag indicating whether the current execution should resume from the last exported PID/Object of process from the beginning.  Only relevant when `fedora.client` is `ocfl`. Default ***`true`***
* `fedora.client`.  {fedora4, ocfl} Client to use for populating a fedora instance.  `fedora4` is an HTTP client used to populate Fedora via its APIs.  `ocfl` is a client that writes OCFL objects to a filesystem, rather than an HTTP API.  They are suitable only for migrating to Fedora 6.  Default: ***`fedora4`***
* `fedora.from.server`.  Host and port of a fedora3, not sure what it is used for.  Default: ***localhost:8080***
* `fedora.to.baseuri`.  For full ldp-based migration (when the `fedora.client` is `fedora4`), the Fedora baseURI you want triples to be migrated to, and/or the Fedora you want to deposit content into.  Default ***http://localhost:${fcrepo.dynamic.test.port:8080}/rest/***
* `foxml.export.dir`: When using the exported foxml layout, this is the directory containing exported foxml.  Default ***src/test/resources/exported***
* `foxml.datastream.dir`.  Datastream directory for legacy and akubra layouts.  Default is ***src/test/resources/legacyFS/datastreams***
* `foxml.object.dir`.  Foxml object dir for legacy and akubra layouts.  Default is ***src/test/resources/legacyFS/objects***

### PID migration selection

The default migration configuration will migrate all of the Fedora 2/3 objects found in the source. Subsequent runs will simply re-migrate all of those objects.
However, there are circumstances when it is preferred that only a subset of all source objects be migrated.
There are three means by which a subset of objects may be selected for migration (noting that these means may also be combined).
* *Limit*: When setting the `limit` configuration (detailed above), the migration will be performed on first X-number of objects specified by the value of `limit`.
* *PID List*: When a pid-list is provided (detailed above), the migration will only be performed on the objects associated with the PIDs in the provided pid-list file.
* *Resume*: When enabling the `resume` configuration (detailed above), a file is maintained that keeps track of the last successfully migration object. Subsequent executions will only migrate objects following the last migrated object. Note, this capability is based on the assumption that the order of objects to be migrated is deterministic and the same from one execution to the next.


### Examples

Run a minimal fedora 6 migration from fedora3 legacy foxml

```shell
java  -Dmigration.strategy=minimal -Dmigration.layout=legacy -Dfedora.client=ocfl -Dmigration.limit=100 \
-Dmigration.ocfl.storage.dir=target/test/ocfl \
-Dmigration.ocfl.staging.dir=/tmp \
-Dfoxml.object.dir=src/test/resources/legacyFS/objects  \
-Dfoxml.datastream.dir=src/test/resources/legacyFS/datastreams  \
-jar target/migration-utils-4.4.1-SNAPSHOT-driver.jar conf/fedora3.xml
```

## Property Mappings

### fcrepo3 Object properties to fcrepo4

| fcrepo 3         | fcrepo4                             | Example                  |
|------------------|-------------------------------------|--------------------------|
| PID              | fedora3model:PID†                   | yul:328697               |
| state            | fedoraaccess:objState               | Active                   |
| label            | fedora3model:label†                 | Elvis Presley            |
| createDate       | fcrepo:created                      | 2015-03-16T20:11:06.683Z |
| lastModifiedDate | fcrepo:lastModified                 | 2015-03-16T20:11:06.683Z |
| ownerId          | fedora3model:ownerId†               | nruest                   |

### fcrepo3 Datastream properties to fcrepo4

| fcrepo3       | fcrepo4                                                      | Example                                                    |
|---------------|--------------------------------------------------------------|------------------------------------------------------------|
| DSID          | dcterms:identifier                                           | OBJ                                                        |
| Label         | dcterms:title‡                                               | ASC19109.tif                                               |
| MIME Type     | ebucore:hasMimeType†                                         | image/tiff                                                 |
| State         | fedoraaccess:objState                                        | Active                                                     |
| Created       | fcrepo:created                                               | 2015-03-16T20:11:06.683Z                                   |
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

### Development

The migration-utils software is built with [Maven 3](https://maven.apache.org) and requires either Java 8 or Java 11.
```bash
mvn clean install
```
The executable utility will be found in the `target` directory.

## Maintainers

Current maintainers

* [Mike Durbin](https://github.com/mikedurbin)
* [Nick Ruest](https://github.com/ruebot)

