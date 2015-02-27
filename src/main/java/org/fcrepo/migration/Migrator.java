package org.fcrepo.migration;

import org.fcrepo.migration.foxml11.FoxmlDirectoryObjectSource;
import org.fcrepo.migration.handlers.ConsoleLoggingStreamingFedoraObjectHandler;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;

/**
 * A class that represents a command-line program to migrate a fedora 3
 * repository to fedora 4.
 *
 * There are two main configuration options: the source and the handler.
 *
 * The source is responsible for exposing objects from a fedora repository,
 * while the handler is responsible for processing each one.
 */
public class Migrator {

    public static void main(String [] args) throws IOException, XMLStreamException {
        if (args.length == 1) {
            File foxmlRoot = new File(args[0]);
            validateDirectory(foxmlRoot);
            System.out.println("Analyzing FOXML in " + foxmlRoot.getPath() + "...");
            Migrator m = new Migrator(new FoxmlDirectoryObjectSource(foxmlRoot),
                    new ConsoleLoggingStreamingFedoraObjectHandler());
            m.run();
        } else if (args.length == 3) {
            File foxmlRoot = new File(args[0]);
            File dsRoot = new File(args[1]);
            File working = new File(args[2]);
            validateDirectory(foxmlRoot);
            validateDirectory(dsRoot);
            System.out.println("Analyzing FOXML in " + foxmlRoot.getPath() + "...");
            System.out.println("Analyzing datastreams in " + dsRoot.getPath() + " and building index within " + working);
            Migrator m = new Migrator(new FoxmlDirectoryObjectSource(foxmlRoot, dsRoot, working),
                    new ConsoleLoggingStreamingFedoraObjectHandler());
            m.run();
        }
    }
    
    private static void validateDirectory(File d) {
        if (!d.exists() || !d.isDirectory()) {
            System.err.println("No directory found at " + d.getAbsolutePath() + "!");
            System.exit(-1);
        }
    }

    private ObjectSource source;

    private StreamingFedoraObjectHandler handler;

    public Migrator(ObjectSource source, StreamingFedoraObjectHandler handler) {
        this.source = source;
        this.handler = handler;
    }

    public void run() throws XMLStreamException {
        for (FedoraObjectProcessor o : source) {
            System.out.println("Processing \"" + o.getObjectInfo().getPid() + "\"...");
            o.processObject(handler);
        }
    }
}
