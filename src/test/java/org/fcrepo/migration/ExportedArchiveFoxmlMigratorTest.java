package org.fcrepo.migration;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.junit.Before;
import org.junit.Test;

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
            result = new DummyHandler();
            fetcher = new DummyURLFetcher();
            new Migrator(new SimpleObjectSource("example1-foxml.xml", getFetcher(), new DummyIDResolver()), getResult()).run();
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
