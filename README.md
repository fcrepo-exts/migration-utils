# Migration Utilities [![Build Status](https://github.com/fcrepo-exts/migration-utils/workflows/Build/badge.svg)](https://github.com/fcrepo-exts/migration-utils/actions)
A framework to support migration of data from Fedora 3 to Fedora 6 repositories

## Overview

This utility iterates the foxml files of a fedora 3 repository, and creates a Fedora 6 compliant OCFL [Oxford Common File Layout](https://ocfl.io)

Migrations to Fedora 6 write migrated objects directly to the filesystem as [OCFL](https://ocfl.io/draft/spec/)
objects.  
In particular:

* There is a 1:1 correspondence between Fedora 3 objects and OCFL objects.  Fedora 3 datastreams appear as files within the resulting OCFL objects.
* RDF is not re-mapped, `info:fedora/` subjects and objects are kept intact as-is
* FOXML object and datastream properties are represented as triples in additional sidecar files

## Status

Fedora 6 support is being actively developed, and is considered unstable until Fedora 6 is released.

## How to use

Background work

* Determine the disposition of your FOXML files:
  * Will you be migrating from exported (archive or migration context) FOXML?
    * If so, you will need all of the export FOXML in a known directory.
  * Will you be migrating from from a native fcrepo3 filesystem?
    * If so, fcrepo3 should not be running, and you will need to determine if you're using legacy or akubra storage

General usage of the migration utils CLI is as follows:

```java -jar target/migration-utils-6.0.0-SNAPSHOT-driver.jar [various options | --help]```

*Note that the migration utility will only run under Java 11+.*  

The following CLI options for specifying details of a given migration are available:
```
Usage: migration-utils [-chrVx] [--debug] -a=<targetDir>
                       [-d=<f3DatastreamsDir>] [-e=<f3ExportedDir>]
                       [-f=<f3hostname>] [-i=<indexDir>] [-l=<objectLimit>]
                       [-m=<migrationType>] [-o=<f3ObjectsDir>] [-p=<pidFile>]
                       -t=<f3SourceType> [-u=<user>] [-U=<userUri>]
  -h, --help                 Show this help message and exit.
  -V, --version              Print version information and exit.
  -t, --source-type=<f3SourceType>
                             Fedora 3 source type. Choices: akubra | legacy |
                               exported
  -d, --datastreams-dir=<f3DatastreamsDir>
                             Directory containing Fedora 3 datastreams (used
                               with --source-type 'akubra' or 'legacy')
  -o, --objects-dir=<f3ObjectsDir>
                             Directory containing Fedora 3 objects (used with
                               --source-type 'akubra' or 'legacy')
  -e, --exported-dir=<f3ExportedDir>
                             Directory containing Fedora 3 export (used with
                               --source-type 'exported')
  -a, --target-dir=<targetDir>
                             Directory where OCFL storage root and supporting
                               state will be written
  -i, --working-dir=<workingDir>
                             Directory where supporting state will be written
                               (cached index of datastreams, ...)
  -I, --delete-inactive      Migrate objects and datastreams in the Inactive
                               state as deleted. Default: false.
  -m, --migration-type=<migrationType>
                             Type of OCFL objects to migrate to. Choices:
                               FEDORA_OCFL | PLAIN_OCFL
                               Default: FEDORA_OCFL
      --id-prefix=<idPrefix> Only use this for PLAIN_OCFL migrations: Prefix to
                               add to PIDs for OCFL object IDs - defaults to
                               info:fedora/, like Fedora3
                               Default: info:fedora/
      --foxml-file           Migrate FOXML file as a whole file, instead of
                               creating property files. FOXML file will be
                               migrated, then marked as deleted so it doesn't
                               show up as an active file.
  -l, --limit=<objectLimit>  Limit number of objects to be processed.
                               Default: no limit
  -r, --resume               Resume from last successfully migrated Fedora 3
                               object
                               Default: false
  -c, --continue-on-error    Continue to next PID if an error occurs (instead
                               of exiting). Disabled by default.
                               Default: false
  -p, --pid-file=<pidFile>   PID file listing which Fedora 3 objects to migrate
  -x, --extensions           Add file extensions to migrated datastreams based
                               on mimetype recorded in FOXML
                               Default: false
  -f, --f3hostname=<f3hostname>
                             Hostname of Fedora 3, used for replacing
                               placeholder in 'E' and 'R' datastream URLs
                               Default: fedora.info
  -u, --username=<user>      The username to associate with all of the migrated
                               resources.
                               Default: fedoraAdmin
  -U, --user-uri=<userUri>   The username to associate with all of the migrated
                               resources.
                               Default: info:fedora/fedoraAdmin
      --algorithm=<digestAlgorithm>
                             The digest algorithm to use in the OCFL objects
                               created. Either sha256 or sha512
                               Default: sha512
      --no-checksum-validation
                             Disable validation that datastream content matches
                               Fedora 3 checksum.
                               Default: false
      --enable-metrics       Enable gathering of metrics for a Prometheus
                               instance.
                             Note: this requires port 8080 to be free in order
                               for Prometheus to scrape metrics.
                               Default: false
      --debug                Enables debug logging
```

### PID migration selection

The default migration configuration will migrate all of the Fedora 3 objects found in the source. Subsequent runs will simply re-migrate all of those objects.
However, there are circumstances when it is preferred that only a subset of all source objects be migrated.
There are three means by which a subset of objects may be selected for migration (noting that these means may also be combined).
* *Limit*: When setting the `limit` configuration (detailed above), the migration will be performed on first X-number of objects specified by the value of `limit`.
* *PID List*: When a pid-list is provided (detailed above), the migration will only be performed on the objects associated with the PIDs in the provided pid-list file.
* *Resume*: When enabling the `resume` configuration (detailed above), a file is maintained that keeps track of the last successfully migration object. Subsequent executions will only migrate objects following the last migrated object. Note, this capability is based on the assumption that the order of objects to be migrated is deterministic and the same from one execution to the next.


### Examples

Run a minimal fedora 6 migration from fedora3 legacy foxml

```shell
java -jar target/migration-utils-6.0.0-SNAPSHOT-driver.jar --source-type=legacy --limit=100 --target-dir=target/test/ocfl --objects-dir=src/test/resources/legacyFS/objects --datastreams-dir=src/test/resources/legacyFS/datastreams
```
Run a minimal fedora 6 migration from a fedora3 archival export
```shell
java -jar target/migration-utils-6.0.0-SNAPSHOT-driver.jar --source-type=exported --limit=100 --target-dir=target/test/ocfl --exported-dir=src/test/resources/exported

```

#### Metrics Gathering

The migration-utils offers some insight into operations using Prometheus and Grafana. When running, the 
`--enable-metrics` option must be used which will start up an HTTP server on port 8080 with an endpoint on `/prometheus`
for Prometheus to scrape data from. This can be tested by going to `http://localhost:8080/prometheus` while the 
migration-utils is running.

To get setup, follow the directions from the [Migration Utils Metrics](https://wiki.lyrasis.org/display/FEDORA6x/Migration+Utils+Metrics)
documentation and run a Fedora 6 migration with metrics enabled:
```shell
java -jar target/migration-utils-6.0.0-SNAPSHOT-driver.jar --source-type=legacy --target-dir=target/test/ocfl --objects-dir=src/test/resources/legacyFS/objects --datastreams-dir=src/test/resources/legacyFS/datastreams --enable-metrics
```

## Property Mappings

### fcrepo 3 Object properties to fcrepo 6+

| fcrepo 3         | fcrepo 6+                           | Example                  |
|------------------|-------------------------------------|--------------------------|
| PID              | fedora3model:PID†                   | yul:328697               |
| state            | fedoraaccess:objState               | Active                   |
| label            | fedora3model:label†                 | Elvis Presley            |
| createDate       | fcrepo:created                      | 2015-03-16T20:11:06.683Z |
| lastModifiedDate | fcrepo:lastModified                 | 2015-03-16T20:11:06.683Z |
| ownerId          | fedora3model:ownerId†               | nruest                   |

### fcrepo3 Datastream properties to fcrepo 4+

| fcrepo 3      | fcrepo 6+                                                    | Example                                                    |
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

| fcrepo 3 event                      | fcrepo 4+ Event Type                            |
|------------------------------------|-------------------------------------------------|
| addDatastream                      | premis:ing‡                                     |
| modifyDatastreamByReference        | audit:contentModification/metadataModification‡ |
| modifyObject                       | audit:resourceModification‡                     |
| modifyObject (checksum validation) | premis:validation‡                              |
| modifyDatastreamByValue            | audit:contentModification/metadataModification‡ |
| purgeDatastream                    | audit:contentRemoval‡                           |

† The `fedora3model` namespace is not a published namespace. It is a representation of the fcrepo 3 namespace `info:fedora/fedora-system:def/model`.

‡ Not yet implemented

**Note**: All fcrepo 3 DC (Dublin Core) datastream values are mapped as dcterms properties on the Object in fcrepo 6+. The same goes for any properties in the RELS-EXT and RELS-INT datastreams.

## Additional Documentation

 * [wiki](https://wiki.duraspace.org/display/FF/Fedora+3+to+4+Data+Migration)

### Development

The migration-utils software is built with [Maven 3](https://maven.apache.org) and requires Java 11 and Maven 3.1+.
```bash
mvn clean install
```
The executable utility will be found in the `target` directory.

## Maintainers

Current maintainer

* [Andrew Woods](https://github.com/awoods)

