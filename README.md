# Migration Utilities

[![Build Status](https://travis-ci.org/fcrepo4-labs/migration-utils.png?branch=master)](https://travis-ci.org/fcrepo4-labs/migration-utils)

A framework to support migration of data from Fedora 3 to Fedora 4 repositories.

## Overview

The main class (`org.fcrepo.migration.Migrator`) iterates over all of the fedora objects in a configured source (`org.fcrepo.migration.ObjectSource`) and handles them using the configured handler (`org.fcrepo.migration.StreamingFedoraObjectHandler`). The configuration is entirely contained within a Spring XML configuration file in [`src/main/resources/spring/migration-bean.xml`](https://github.com/fcrepo4-labs/migration-utils/blob/master/src/main/resources/spring/migration-bean.xml).

## Status

A basic migration scenario is implemented that may serve as a starting point for
your own migration from Fedora 3.x to Fedora 4.x.

## Usage

To run the migration scenario you have configured in the Spring XML configuration file: 

``` 
mvn clean compile exec:java -Dexec.mainClass=org.fcrepo.migration.Migrator 
```

## Additional Documentation
 
 * [wiki](https://wiki.duraspace.org/display/FF/Fedora+3+to+4+Data+Migration)
