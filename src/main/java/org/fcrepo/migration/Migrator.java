package org.fcrepo.migration;

import org.slf4j.Logger;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;

import static org.slf4j.LoggerFactory.getLogger;

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

    private static final Logger LOGGER = getLogger(Migrator.class);
    
    public static void main(final String [] args) throws IOException, XMLStreamException {

        final ConfigurableApplicationContext context = new ClassPathXmlApplicationContext("spring/migration-bean.xml");
        final Migrator m = context.getBean("migrator", Migrator.class);
        m.run();
        context.close();
    }

    private ObjectSource source;

    private StreamingFedoraObjectHandler handler;

    private int limit;

    public Migrator() {
        limit = -1;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public void setSource(final ObjectSource source) {
        this.source = source;
    }


    public void setHandler(final StreamingFedoraObjectHandler handler) {
        this.handler = handler;
    }

    public Migrator(final ObjectSource source, final StreamingFedoraObjectHandler handler) {
        this();
        this.source = source;
        this.handler = handler;
    }

    public void run() throws XMLStreamException {
        int index = 0;
        for (final FedoraObjectProcessor o : source) {
            if (limit >= 0 && index ++ >= limit) {
                break;
            }
            LOGGER.info("Processing \"" + o.getObjectInfo().getPid() + "\"...");
            o.processObject(handler);
        }
    }
}
