package grakn.core.server.session.cache;

import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;


/**
 *
 * Interesting takeaways:
 * - Creating a new type, we retrieve and cache the entire hierarchy up to `Thing` (top level meta concept)
 * - When fetching an existing type, we only cache that single type, not the entire hierarchy
 * - When fetching instances of a type, we cache both the instances and that type (but again, no supertypes)
 *
 */

@SuppressWarnings("CheckReturnValue")
public class TransactionCacheIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private TransactionOLTP tx;
    private SessionImpl session;

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        tx = session.transaction().write();
    }

    @After
    public void tearDown() {
        tx.close();
        session.close();
    }


    @Test
    public void whenOpenTransaction_ConceptCacheIsEmpty() {
        TransactionCache transactionCache = tx.cache();
        Map<ConceptId, Concept> conceptCache = transactionCache.getConceptCache();
        assertEquals(0, conceptCache.size());
    }

    @Test
    public void whenOpenTransaction_SchemaConceptCacheIsEmpty() {
        TransactionCache transactionCache = tx.cache();
        Map<Label, SchemaConcept> schemaConceptCache = transactionCache.getSchemaConceptCache();
        assertEquals(0, schemaConceptCache.size());
    }

    @Test
    public void whenQueryForConcept_ConceptCacheIsUpdated() {
        // define and insert a concept
        EntityType person = tx.putEntityType("person");
        person.create();
        tx.commit();

        // examine new transaction's cache
        tx = session.transaction().read();
        EntityType retrievedPerson = tx.getEntityType("person");
        Entity retrievedPersonInstance = retrievedPerson.instances().collect(Collectors.toList()).get(0);

        TransactionCache transactionCache = tx.cache();
        Map<ConceptId, Concept> conceptCache = transactionCache.getConceptCache();

        // concept cache contains "person" type and "person" instance
        assertEquals(2, conceptCache.size());
        assertThat(conceptCache.values(), containsInAnyOrder(retrievedPerson, retrievedPersonInstance));
    }

    @Test
    public void whenQueryForSchemaConcept_SchemaConceptCacheIsUpdated() {
        // define and insert a concept
        EntityType person = tx.putEntityType("person");
        tx.commit();

        // examine new transaction's cache
        tx = session.transaction().read();
        tx.getEntityType("person");

        TransactionCache transactionCache = tx.cache();
        Map<Label, SchemaConcept> schemaConceptCache = transactionCache.getSchemaConceptCache();

        assertEquals(1, schemaConceptCache.size());
        assertEquals("person", schemaConceptCache.values().iterator().next().label().toString());
    }

    @Test
    public void whenCreateNewConcept_ConceptCacheIsUpdated() {
        // define and insert a concept
        EntityType person = tx.putEntityType("person");
        Entity personInstance = person.create();

        TransactionCache transactionCache = tx.cache();
        Map<ConceptId, Concept> conceptCache = transactionCache.getConceptCache();

        assertEquals(4, conceptCache.size());

        List<String> conceptCacheLabels = conceptCache.values().stream().map(concept -> {
            if (concept.isThing()) {
                return concept.asThing().type().label().toString();
            } else {
                return concept.asType().label().toString();
            }
        }).collect(Collectors.toList());
        assertThat(conceptCacheLabels, containsInAnyOrder("person", "person", "entity", "thing"));
    }

    @Test
    public void whenCreateNewSchemaConcept_SchemaConceptCacheIsUpdated() {
        // define a concept type
        EntityType person = tx.putEntityType("person");

        TransactionCache transactionCache = tx.cache();
        Map<Label, SchemaConcept> schemaConceptCache = transactionCache.getSchemaConceptCache();

        /*
        Expect 3 types in the schema concept cache because:
        1. `thing` is required to create the next type label ID
        2. Hierarchy of supertypes is immediates retrieved and cache on new type creation
         */
        assertEquals(3, schemaConceptCache.size());
        List<String> cachedSchemaConceptLabels = schemaConceptCache.values().stream().map(schemaConcept -> schemaConcept.label().toString()).collect(Collectors.toList());
        assertThat(cachedSchemaConceptLabels, containsInAnyOrder("person", "entity", "thing"));
    }

    @Test
    public void whenDeleteConcept_ConceptCacheIsUpdated() {
        // define and insert a concept
        EntityType person = tx.putEntityType("person");
        Entity personInstance = person.create();

        TransactionCache transactionCache = tx.cache();
        Map<ConceptId, Concept> conceptCache = transactionCache.getConceptCache();

        personInstance.delete();

        List<String> cachedConceptLabels = conceptCache.values().stream().map(concept-> concept.asType().label().toString()).collect(Collectors.toList());
        assertEquals(3, conceptCache.size());
        assertThat(cachedConceptLabels,containsInAnyOrder("person", "entity", "thing"));
    }

    @Test
    public void whenDeleteSchemaConcept_SchemaConceptCacheIsUpdated() {
        // define and insert a concept
        EntityType person = tx.putEntityType("person");

        TransactionCache transactionCache = tx.cache();
        Map<Label, SchemaConcept> schemaConceptCache = transactionCache.getSchemaConceptCache();

        person.delete();

        assertEquals(2, schemaConceptCache.size());
        List<String> cachedSchemaConceptLabels = schemaConceptCache.values().stream().map(schemaConcept -> schemaConcept.label().toString()).collect(Collectors.toList());
        assertThat(cachedSchemaConceptLabels, containsInAnyOrder( "entity", "thing"));
    }
}
