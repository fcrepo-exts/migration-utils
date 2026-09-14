/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 *
 */
package org.fcrepo.migration;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * A series of tests that cover all the features used in processing
 * FOXML exported using the context=archive option.
 * @author mdurbin
 */
public class ExportedArchiveFoxmlMigratorTest extends Example1TestSuite {

    private static DummyHandler result;

    private static DummyURLFetcher fetcher;

    @BeforeEach
    public synchronized void processFoxml() throws XMLStreamException, IOException {
        if (getResult() == null) {
            final ConfigurableApplicationContext context =
                    new ClassPathXmlApplicationContext("spring/exported-foxml.xml");
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

    @Test
    public void testTempFileRemoval() throws IOException {
        assertThrows(IllegalStateException.class, () ->
            getResult().dsVersions.get(4).getContent());
    }
}
