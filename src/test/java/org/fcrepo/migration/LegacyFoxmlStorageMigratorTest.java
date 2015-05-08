package org.fcrepo.migration;

import org.junit.Before;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;

/**
 * @author mdurbin
 */
public class LegacyFoxmlStorageMigratorTest extends Example1TestSuite {

    private static DummyHandler result;

    private static DummyURLFetcher fetcher;

    @Before
    public synchronized void processFoxml() throws XMLStreamException, IOException {
        if (getResult() == null) {
            final ConfigurableApplicationContext context =
                    new ClassPathXmlApplicationContext("spring/stored-legacy-foxml.xml");
            this.result = (DummyHandler) context.getBean("dummyHandler");
            this.fetcher = (DummyURLFetcher) context.getBean("dummyFetcher");
            final Migrator m = (Migrator) context.getBean("migrator");
            m.run();
            context.close();
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
