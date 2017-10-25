package ai.grakn.test.migration.xml;

import ai.grakn.Grakn;
import ai.grakn.GraknTx;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.EntityType;
import ai.grakn.migration.base.Migrator;
import ai.grakn.migration.xml.XmlMigrator;
import ai.grakn.test.EngineContext;
import ai.grakn.test.migration.MigratorTestUtils;
import ai.grakn.util.SampleKBLoader;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

/**
 * Testing the XML Migrator class
 *
 * @author alexandraorth
 */
public class XMLMigratorTest {

    private static Keyspace keyspace;
    private static GraknSession session;

    @ClassRule
    public static final EngineContext engine = EngineContext.createWithInMemoryRedis();

    @BeforeClass
    public static void loadSchema(){
        keyspace = SampleKBLoader.randomKeyspace();
        session = Grakn.session(engine.uri(), keyspace);
    }

    @After
    public void clearGraph(){
        try(GraknTx graph = session.open(GraknTxType.WRITE)){
            AttributeType<String> nameType = graph.getAttributeType("name");
            nameType.instances().forEach(Concept::delete);

            EntityType thingType = graph.getEntityType("thingy");
            thingType.instances().forEach(Concept::delete);

            graph.commit();
        }
    }

    @Test
    public void whenMigratingXML_CanMigrateXMLAttributes(){
        String template = "insert $thing isa thingy has name <\"~NAME\">;";
        migrateXMLWithElement("THINGY", template);

        assertThingHasName("Bob");
    }

    @Test
    public void whenMigratingXML_CanMigrateTextInPrimaryNode(){
        String template = "insert $thing isa thingy has name <textContent>;";
        migrateXMLWithElement("THINGY", template);

        assertThingHasName("innerText");
    }

    @Test
    public void whenMigratingXML_CanMigrateXMLAttributesInChildNodes(){
        String template = "insert $thing isa thingy has name <NAME[1].\"~NAME\">;";
        migrateXMLWithElement("THINGY", template);

        assertThingHasName("Alice");
    }

    @Test
    public void whenMigratingXML_CanMigrateXMLTextInChildNodes(){
        String template = "insert $thing isa thingy has name <NAME[0].textContent>;";
        migrateXMLWithElement("THINGY", template);

        assertThingHasName("Charlie");
    }

    private static void assertThingHasName(String name){
        try(GraknTx graph = session.open(GraknTxType.READ)){

            EntityType thingType = graph.getEntityType("thingy");
            AttributeType nameType = graph.getAttributeType("name");

            assertEquals(1, thingType.instances().count());
            thingType.instances().forEach(thing ->{
                assertEquals(1, thing.attributes(nameType).count());
                assertEquals(name, thing.attributes(nameType).iterator().next().getValue());
            });
        }
    }

    private static void migrateXMLWithElement(String element, String template){
        // load the schema
        MigratorTestUtils.load(session, MigratorTestUtils.getFile("xml", "schema.gql"));

        // load the data
        Migrator migrator = new Migrator(engine.uri(), keyspace);

        File xmlFile = MigratorTestUtils.getFile("xml", "data.xml");

        XmlMigrator xmlMigrator = new XmlMigrator(xmlFile);
        xmlMigrator.element(element);

        migrator.load(template, xmlMigrator.convert());
    }
}
