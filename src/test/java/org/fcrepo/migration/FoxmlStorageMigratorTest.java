package org.fcrepo.migration;

import org.fcrepo.migration.foxml11.DirectoryScanningIDResolver;
import org.junit.Before;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;

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
            new Migrator(new SimpleObjectSource("info%3Afedora%2Fexample%3A1", getFetcher(),
                    new DirectoryScanningIDResolver(new File("target/index"),
                    new File("src/test/resources/datastreamStore"))), getResult()).run();
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
