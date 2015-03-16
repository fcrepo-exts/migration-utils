package org.fcrepo.migration;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.junit.Before;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * A series of tests that cover all the features used in processing
 * FOXML found in a fedora objectStore directory.
 */
public class FoxmlStorageMigratorTest extends Example1TestSuite {

    private static DummyHandler result;

    private static DummyURLFetcher fetcher;

    @Before
    public synchronized void processFoxml() throws XMLStreamException, IOException {
        if (getResult() == null) {
            this.result = new DummyHandler();
            this.fetcher = new DummyURLFetcher();
            /* new Migrator(new SimpleObjectSource("info%3Afedora%2Fexample%3A1", getFetcher(),
                    new DirectoryScanningIDResolver(new File("target/index"),
                    new File("src/test/resources/datastreamStore"))), getResult()).run();*/
            final ApplicationContext context = new ClassPathXmlApplicationContext("spring/migration-bean.xml");
            final Migrator m = (Migrator)context.getBean("migratorTest");
            m.run();
            ((ConfigurableApplicationContext)context).close();

        }
    }

    @Override
    protected DummyHandler getResult() {
        return result;
    }

    @Override
    protected DummyURLFetcher getFetcher() {
        return fetcher;
    }
}
