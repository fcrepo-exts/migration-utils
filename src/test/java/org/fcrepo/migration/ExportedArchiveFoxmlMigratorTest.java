package org.fcrepo.migration;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * A series of tests that cover all the features used in processing
 * FOXML exported using the context=archive option.
 */
public class ExportedArchiveFoxmlMigratorTest extends Example1TestSuite {

    private static DummyHandler result;

    private static DummyURLFetcher fetcher;

    @Before
    public synchronized void processFoxml() throws XMLStreamException, IOException {
        if (getResult() == null) {
            final ConfigurableApplicationContext context = new ClassPathXmlApplicationContext("spring/exported-foxml.xml");
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

    @Test (expected = IllegalStateException.class)
    public void testTempFileRemoval() throws IOException {
        getResult().dsVersions.get(4).getContent();
    }
}
