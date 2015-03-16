package org.fcrepo.migration;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

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

    public static void main(final String [] args) throws IOException, XMLStreamException {

        final ConfigurableApplicationContext context = new ClassPathXmlApplicationContext("spring/migration-bean.xml");
        final Migrator m = context.getBean("migrator", Migrator.class);
        m.run();
        context.close();
    }

    private ObjectSource source;

    private StreamingFedoraObjectHandler handler;

    public Migrator() {

    }

    public void setSource(final ObjectSource source) {
        this.source = source;
    }


    public void setHandler(final StreamingFedoraObjectHandler handler) {
        this.handler = handler;
    }

    public Migrator(final ObjectSource source, final StreamingFedoraObjectHandler handler) {
        this.source = source;
        this.handler = handler;
    }

    public void run() throws XMLStreamException {
        for (final FedoraObjectProcessor o : source) {
            System.out.println("Processing \"" + o.getObjectInfo().getPid() + "\"...");
            o.processObject(handler);
        }
    }
}
