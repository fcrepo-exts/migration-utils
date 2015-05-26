# Migration Utilities [![Build Status](https://travis-ci.org/fcrepo4-labs/migration-utils.png?branch=master)](https://travis-ci.org/fcrepo4-labs/migration-utils)

A framework to support migration of data from Fedora 3 to Fedora 4 repositories.

## Overview

The main class (`org.fcrepo.migration.Migrator`) iterates over all of the fedora objects in a configured source (`org.fcrepo.migration.ObjectSource`) and handles them using the configured handler (`org.fcrepo.migration.StreamingFedoraObjectHandler`). The configuration is entirely contained within a Spring XML configuration file in [`src/main/resources/spring/migration-bean.xml`](https://github.com/fcrepo4-labs/migration-utils/blob/master/src/main/resources/spring/migration-bean.xml).

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
* Determine your fcrepo4 url (ex: http://localhost:8080/rest/, http://yourHostName.ca:8080/fcrepo/rest/) ([line 140](https://github.com/fcrepo4-labs/migration-utils/blob/master/src/main/resources/spring/migration-bean.xml#L140)
* There is currently only one implemented pid-mapping strategy, but you can configure it to put all of your migrated content under a given path ([line 93](https://github.com/fcrepo4-labs/migration-utils/blob/master/src/main/resources/spring/migration-bean.xml#L93), sets that value to "migrated-fedora3").

Getting started:
* [Download](https://github.com/fcrepo4-labs/migration-utils/releases) the executable jar file
* Create a local copy of the example [configuration file](https://github.com/fcrepo4-labs/migration-utils/blob/master/src/main/resources/spring/migration-bean.xml) and update as described below:
  * If you are migrating from exported FOXML, you will leave [line 9](https://github.com/fcrepo4-labs/migration-utils/blob/master/src/main/resources/spring/migration-bean.xml#L9).
  * If you are migrating from a native fcrepo3 file system, you will need to change `exportedFoxmlDirectoryObjectSource` to `nativeFoxmlDirectoryObjectSource` in [line 9](https://github.com/fcrepo4-labs/migration-utils/blob/master/src/main/resources/spring/migration-bean.xml#L9).
  * If you are migrating from a native fcrepo3 file system, you will need to set the paths to the `objectStore` and `datastreamStore` ([Lines 143-139](https://github.com/fcrepo4-labs/migration-utils/blob/master/src/main/resources/spring/migration-bean.xml#L143-L149)).
  * If you are migrating from exported FOXML, you will need to set the path to the directory you have them stored in ([Lines 151-153](https://github.com/fcrepo4-labs/migration-utils/blob/master/src/main/resources/spring/migration-bean.xml#L151-L153)).
  * Set your fcrepo4 url ([Line 140](https://github.com/fcrepo4-labs/migration-utils/blob/master/src/main/resources/spring/migration-bean.xml#L140)).
  * If you would like to run the migration in test mode (console logging), you will leave [lines 11-16](https://github.com/fcrepo4-labs/migration-utils/blob/master/src/main/resources/spring/migration-bean.xml#L11-L16) as is.
  * If you would like to run the migration, you will need to comment out or remove [line 9](https://github.com/fcrepo4-labs/migration-utils/blob/master/src/main/resources/spring/migration-bean.xml#L11), and uncomment [line 15](https://github.com/fcrepo4-labs/migration-utils/blob/master/src/main/resources/spring/migration-bean.xml#L15).


To run the migration scenario you have configured in the Spring XML configuration file:

```
java -jar migration-utils-{version}-driver.jar <relative-or-absolute-path-to-configuration-file>
```

## Property Mappings

### fcrepo3 Object properties to fcrepo4

| fcrepo 3         | fcrepo4                             | Example                  |
|------------------|-------------------------------------|--------------------------|
| PID              | dcterms:identifier                  | yul:328697               |
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
