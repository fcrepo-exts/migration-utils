package org.fcrepo.migration;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.slf4j.Logger;
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
 * @author mdurbin
 */
public class Migrator {

    private static final Logger LOGGER = getLogger(Migrator.class);

    /**
     * the main method.
     * @param args the arguments
     * @throws IOException IO exception
     * @throws XMLStreamException xml stream exception
     */
    public static void main(final String [] args) throws IOException, XMLStreamException {

        final ConfigurableApplicationContext context = new ClassPathXmlApplicationContext("spring/migration-bean.xml");
        final Migrator m = context.getBean("migrator", Migrator.class);
        m.run();
        context.close();
    }

    private ObjectSource source;

    private StreamingFedoraObjectHandler handler;

    private int limit;

    /**
     * the migrator. set limit to -1.
     */
    public Migrator() {
        limit = -1;
    }

    /**
     * set the limit.
     * @param limit the limit
     */
    public void setLimit(final int limit) {
        this.limit = limit;
    }

    /**
     * set the source.
     * @param source the object source
     */
    public void setSource(final ObjectSource source) {
        this.source = source;
    }


    /**
     * set the handler.
     * @param handler the handler
     */
    public void setHandler(final StreamingFedoraObjectHandler handler) {
        this.handler = handler;
    }

    /**
     * The constructor for migrator.
     * @param source the source
     * @param handler the handler
     */
    public Migrator(final ObjectSource source, final StreamingFedoraObjectHandler handler) {
        this();
        this.source = source;
        this.handler = handler;
    }

    /**
     * the run method for migrator.
     * @throws XMLStreamException xml stream exception
     */
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
