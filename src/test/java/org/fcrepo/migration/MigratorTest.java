/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.migration;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import javax.xml.stream.XMLStreamException;

import org.fcrepo.migration.pidlist.ResumePidListManager;
import org.fcrepo.migration.pidlist.UserProvidedPidListManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit tests for {@link Migrator#run()} covering limit handling, pid filtering
 * and error handling behaviour.
 *
 * @author Dan Field
 */
@RunWith(MockitoJUnitRunner.class)
public class MigratorTest {

    @Mock
    private ObjectSource source;

    @Mock
    private StreamingFedoraObjectHandler handler;

    @Mock
    private ResumePidListManager resumePidListManager;

    @Mock
    private UserProvidedPidListManager userProvidedPidListManager;

    private Migrator migrator;

    @Before
    public void setup() {
        migrator = new Migrator();
        migrator.setHandler(handler);
        migrator.setSource(source);
    }

    private FedoraObjectProcessor processor(final String pid) {
        final ObjectInfo info = Mockito.mock(ObjectInfo.class);
        Mockito.lenient().when(info.getPid()).thenReturn(pid);
        final FedoraObjectProcessor processor = Mockito.mock(FedoraObjectProcessor.class);
        Mockito.lenient().when(processor.getObjectInfo()).thenReturn(info);
        return processor;
    }

    @SafeVarargs
    private final void sourceReturns(final FedoraObjectProcessor... processors) {
        Mockito.when(source.iterator()).thenReturn(Arrays.asList(processors).iterator());
    }

    @Test
    public void processesAllObjects() throws Exception {
        final var p1 = processor("obj:1");
        final var p2 = processor("obj:2");
        sourceReturns(p1, p2);

        migrator.run();

        Mockito.verify(p1).processObject(handler);
        Mockito.verify(p2).processObject(handler);
        Mockito.verify(p1).close();
        Mockito.verify(p2).close();
    }

    @Test
    public void respectsLimit() throws Exception {
        final var p1 = processor("obj:1");
        final var p2 = processor("obj:2");
        sourceReturns(p1, p2);
        migrator.setLimit(1);

        migrator.run();

        Mockito.verify(p1).processObject(handler);
        Mockito.verify(p2, Mockito.never()).processObject(handler);
    }

    @Test
    public void skipsObjectsWithNullPid() throws Exception {
        final var p1 = processor(null);
        sourceReturns(p1);

        migrator.run();

        Mockito.verify(p1, Mockito.never()).processObject(handler);
    }

    @Test
    public void continueOnErrorSwallowsProcessingFailure() throws Exception {
        final var p1 = processor("obj:1");
        final var p2 = processor("obj:2");
        Mockito.doThrow(new XMLStreamException("boom")).when(p1).processObject(handler);
        sourceReturns(p1, p2);
        migrator.setContinueOnError(true);

        migrator.run();

        Mockito.verify(p2).processObject(handler);
    }

    @Test(expected = RuntimeException.class)
    public void processingFailureThrowsWhenNotContinuing() throws Exception {
        final var p1 = processor("obj:1");
        Mockito.doThrow(new XMLStreamException("boom")).when(p1).processObject(handler);
        sourceReturns(p1);

        migrator.run();
    }

    @Test
    public void continueOnErrorSwallowsUnreadableObject() throws Exception {
        @SuppressWarnings("unchecked")
        final Iterator<FedoraObjectProcessor> iterator = Mockito.mock(Iterator.class);
        Mockito.when(iterator.hasNext()).thenReturn(true, false);
        Mockito.when(iterator.next()).thenThrow(new RuntimeException("unreadable"));
        Mockito.when(source.iterator()).thenReturn(iterator);
        migrator.setContinueOnError(true);

        migrator.run();

        Mockito.verify(iterator, Mockito.times(2)).hasNext();
    }

    @Test(expected = RuntimeException.class)
    public void unreadableObjectThrowsWhenNotContinuing() throws Exception {
        @SuppressWarnings("unchecked")
        final Iterator<FedoraObjectProcessor> iterator = Mockito.mock(Iterator.class);
        Mockito.when(iterator.hasNext()).thenReturn(true, false);
        Mockito.when(iterator.next()).thenThrow(new RuntimeException("unreadable"));
        Mockito.when(source.iterator()).thenReturn(iterator);

        migrator.run();
    }

    @Test
    public void stopsWhenUserProvidedListFinished() throws Exception {
        final var p1 = processor("obj:1");
        final var p2 = processor("obj:2");
        sourceReturns(p1, p2);
        migrator.setUserProvidedPidListManager(userProvidedPidListManager);
        Mockito.when(userProvidedPidListManager.accept(Mockito.anyString())).thenReturn(true);
        Mockito.when(userProvidedPidListManager.finishedProcessingAllPids()).thenReturn(true);

        migrator.run();

        Mockito.verify(p1).processObject(handler);
        Mockito.verify(p2, Mockito.never()).processObject(handler);
    }

    @Test
    public void skipsPidRejectedByUserProvidedList() throws Exception {
        final var p1 = processor("obj:1");
        sourceReturns(p1);
        migrator.setUserProvidedPidListManager(userProvidedPidListManager);
        Mockito.when(userProvidedPidListManager.accept("obj:1")).thenReturn(false);
        Mockito.when(userProvidedPidListManager.finishedProcessingAllPids()).thenReturn(false);

        migrator.run();

        Mockito.verify(p1, Mockito.never()).processObject(handler);
    }

    @Test
    public void skipsPidRejectedByResumeList() throws Exception {
        final var p1 = processor("obj:1");
        sourceReturns(p1);
        migrator.setResumePidListManager(resumePidListManager);
        Mockito.when(resumePidListManager.accept("obj:1")).thenReturn(false);

        migrator.run();

        Mockito.verify(p1, Mockito.never()).processObject(handler);
    }

    @Test
    public void mainWithoutConfigPrintsHelp() throws Exception {
        // No arguments -> the usage/help text is printed and the method returns without error.
        Migrator.main(new String[] {});
    }

    @Test
    public void constructorWiresSourceAndHandler() throws Exception {
        final var p1 = processor("obj:1");
        Mockito.when(source.iterator()).thenReturn(Collections.singletonList(p1).iterator());
        final var constructed = new Migrator(source, handler);

        constructed.run();

        Mockito.verify(p1).processObject(handler);
    }
}
