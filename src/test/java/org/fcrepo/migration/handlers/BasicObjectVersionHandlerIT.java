package org.fcrepo.migration.handlers;

import junit.framework.Assert;
import org.fcrepo.client.FedoraException;
import org.fcrepo.client.FedoraRepository;
import org.fcrepo.migration.Migrator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.xml.stream.XMLStreamException;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author mdurbin
 */
public class BasicObjectVersionHandlerIT {

    final static Logger LOGGER = getLogger(BasicObjectVersionHandlerIT.class);

    private static FedoraRepository repo;

    @BeforeClass
    public static void migrateTestData() throws XMLStreamException {
        final ConfigurableApplicationContext context =
                new ClassPathXmlApplicationContext("spring/it-setup.xml");
        final Migrator m = (Migrator) context.getBean("migrator");
        repo = (FedoraRepository) context.getBean("fedora4Client");
        m.run();
        context.close();
    }

    @Test
    public void testObjectsWereCreated() throws FedoraException {
        Assert.assertTrue(repo.exists("migrated-fedora3/example/1"));
        Assert.assertTrue(repo.exists("migrated-fedora3/example/2"));
        Assert.assertTrue(repo.exists("migrated-fedora3/example/3"));
    }

    @Test
    public void testPlaceholdersWereCreated() throws FedoraException {
        Assert.assertTrue(repo.exists("migrated-fedora3/cmodel/1"));
    }

    @Test
    public void testExample1DatastreamsWereCreated() throws FedoraException {
        for (String dsid : new String[] { "DS1", "DS2", "DS3", "DS4" }) {
            Assert.assertTrue(repo.exists("migrated-fedora3/example/1/" + dsid));
        }
    }

}
