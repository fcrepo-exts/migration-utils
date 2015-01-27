A framework to support migration of data from Fedora 3 to Fedora 4 repositories.

# Overview

The basic program allows for a configuration to define one or more Fedora Object Handlers and
a repository source.  The handlers will in turn be provided information about each each object
in the repository under the theory that one ore more Handler implementations may be written to
achieve whatever complex data migration or analysis is desired.

# Status

Currently this application is far from ready for use, but the basic framework is presented to allow:

1.  testing to ensure that it does indeed completely and accurately parse FOXML from various fedora versions and contexts
2.  the framework to be discussed and improved before complex mappings and migration scenarios are implemented

# Usage

To get basic output for a directory of FOXML that have been produced using the REST API's export with the "archive" context:

``` mvn clean compile exec:java -Dexec.mainClass=org.fcrepo.migration.Migrator -Dexec.args="path/to/exported/foxml" ```

To get basic output for a directory of FOXML read from fedora's storage:

``` mvn clean compile exec:java -Dexec.mainClass=org.fcrepo.migration.Migrator -Dexec.args="path/to/fedora/data/objectStore path/to/fedora/data/datastreamStore work/directory" ```

# Development

To do something more sophisticated (like actually migrating or analyzing content) implement
FedoraObjectHandler and update Migrator before running.

# Additional Documentation
 * [wiki](https://wiki.duraspace.org/display/FF/Fedora+3+to+4+Data+Migration)






