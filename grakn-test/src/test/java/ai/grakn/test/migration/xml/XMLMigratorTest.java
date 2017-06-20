package ai.grakn.test.migration.xml;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.ResourceType;
import ai.grakn.migration.base.Migrator;
import ai.grakn.migration.xml.XmlMigrator;
import ai.grakn.test.EngineContext;
import ai.grakn.test.migration.MigratorTestUtils;
import ai.grakn.util.GraphLoader;
import java.io.File;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Testing the XML Migrator class
 *
 * @author alexandraorth
 */
public class XMLMigratorTest {

    private static String keyspace;
    private static GraknSession session;

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @BeforeClass
    public static void loadOntology(){
        keyspace = GraphLoader.randomKeyspace();
        session = Grakn.session(engine.uri(), keyspace);

        migrateXMLWithElement("THING");
    }

    @Test
    public void whenMigratingXML_CanMigrateXMLAttributes(){
        try(GraknGraph graph = session.open(GraknTxType.READ)){

            ResourceType priceType = graph.getResourceType("name");
            EntityType thingType = graph.getEntityType("thing");

            for(Entity thing:thingType.instances()){
                assertEquals(1, thing.resources(priceType).size());
            }
        }
    }

    @Test
    public void whenMigratingXML_CanMigrateTextInTags(){
        try(GraknGraph graph = session.open(GraknTxType.READ)){

            ResourceType nameType = graph.getResourceType("name");
            EntityType thingType = graph.getEntityType("thing");
            assertEquals(3, thingType.instances().size());

            for(Entity thing:thingType.instances()){
                assertEquals(1, thing.resources(nameType).size());
            }
        }
    }

    private static void migrateXMLWithElement(String element){
        // load the ontology
        MigratorTestUtils.load(session, MigratorTestUtils.getFile("xml", "no-attributes/ontology.gql"));

        // load the data
        Migrator migrator = Migrator.to(engine.uri(), keyspace);

        File xmlFile = MigratorTestUtils.getFile("xml", "no-attributes/data.xml");
        String template = MigratorTestUtils.getFileAsString("xml", "no-attributes/template.gql");

        XmlMigrator xmlMigrator = new XmlMigrator(xmlFile);
        xmlMigrator.element(element);

        migrator.load(template, xmlMigrator.convert());
    }
}
