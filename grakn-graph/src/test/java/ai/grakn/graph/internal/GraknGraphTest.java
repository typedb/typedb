package ai.grakn.graph.internal;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.ErrorMessage.TRANSACTIONS_OPEN;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class GraknGraphTest extends GraphTestBase {

    @Test
    public void openMultipleFakeNestedTransactions_CheckCloseOnlyOccursOnCorrectClose(){
        GraknGraphFactory factory = Grakn.factory(Grakn.IN_MEMORY, "BobGraph");

        //Open One Close One
        GraknGraph graph = factory.getGraph();
        assertFalse(graph.isClosed());
        graph.close();
        assertTrue(graph.isClosed());

        //Open Two Close One
        factory.getGraph(); //Due to the singleton nature of the factory this alone is enough to increment the count
        graph = factory.getGraph();
        assertFalse(graph.isClosed());
        graph.close();
        assertFalse(graph.isClosed());

        //Close final one
        graph.close();
        assertTrue(graph.isClosed());

        //Open Three Close One Try to Close Factory And Fail
        factory.getGraph();
        graph = factory.getGraph();
        graph.close();

        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(TRANSACTIONS_OPEN.getMessage(graph, graph.getKeyspace(), 2));
        factory.close();
    }

    @Test
    public void testPutConcept() throws Exception {
        int numVerticies = 14;
        for(int i = 0; i < numVerticies; i ++)
            graknGraph.putEntityType("c" + i);
        assertEquals(22, graknGraph.getTinkerPopGraph().traversal().V().toList().size());
    }

    @Test
    public void testGetConceptByBaseIdentifier() throws Exception {
        assertNull(graknGraph.getConceptByBaseIdentifier(1000L));
        EntityType c1 = graknGraph.putEntityType("c1");
        Concept c2 = graknGraph.getConceptByBaseIdentifier(c1.getId());
        assertEquals(c1, c2);
    }

    @Test
    public void testGetConcept() throws Exception {
        EntityType entityType = graknGraph.putEntityType("VALUE");
        assertEquals(entityType, graknGraph.getConcept(entityType.getId()));

        Entity entity = entityType.addEntity();
        assertEquals(entity, graknGraph.getConcept(entity.getId()));
    }

    @Test
    public void testReadOnlyTraversal(){
        expectedException.expect(VerificationException.class);
        expectedException.expectMessage("not read only");
        graknGraph.getTinkerTraversal().drop().iterate();
    }

    @Test
    public void testAddCastingWithDuplicates() {
        //Artificially Make First Casting
        RelationType relationType = graknGraph.putRelationType("RelationType");
        RoleTypeImpl role = (RoleTypeImpl) graknGraph.putRoleType("role-thing");
        EntityType thing = graknGraph.putEntityType("thing");
        EntityImpl rolePlayer = (EntityImpl) thing.addEntity();
        RelationImpl relation = (RelationImpl) relationType.addRelation();

        //First Casting
        makeArtificialCasting(role, rolePlayer, relation);

        //Second Casting Between same entities
        makeArtificialCasting(role, rolePlayer, relation);

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(ErrorMessage.TOO_MANY_CASTINGS.getMessage(role, rolePlayer));

        graknGraph.putCasting(role, rolePlayer, relation);
    }
    private void makeArtificialCasting(RoleTypeImpl role, InstanceImpl rolePlayer, RelationImpl relation) {
        Vertex vertex = graknGraph.getTinkerPopGraph().addVertex(Schema.BaseType.CASTING.name());
        String id = vertex.id().toString();

        vertex.property(Schema.ConceptProperty.INDEX.name(), CastingImpl.generateNewHash(role, rolePlayer));
        vertex.property(Schema.ConceptProperty.ID.name(), id);

        CastingImpl casting = graknGraph.getConcept(ConceptId.of(id));
        EdgeImpl edge = casting.addEdge(role, Schema.EdgeLabel.ISA); // Casting to Role
        edge.setProperty(Schema.EdgeProperty.ROLE_TYPE, role.getId().getValue());
        edge = casting.addEdge(rolePlayer, Schema.EdgeLabel.ROLE_PLAYER);// Casting to Roleplayer
        edge.setProperty(Schema.EdgeProperty.ROLE_TYPE, role.getId().getValue());
        relation.addEdge(casting, Schema.EdgeLabel.CASTING);// Assertion to Casting
    }

    @Test
    public void testGetResourcesByValue(){
        assertEquals(0, graknGraph.getResourcesByValue("Bob").size());
        ResourceType<String> type = graknGraph.putResourceType("Parent", ResourceType.DataType.STRING);
        ResourceType<String> type2 = graknGraph.putResourceType("Parent 2", ResourceType.DataType.STRING);

        Resource<String> c1 = type.putResource("Bob");
        Resource<String> c2 = type2.putResource("Bob");
        Resource<String> c3 = type.putResource("Bob");

        assertEquals(2, graknGraph.getResourcesByValue("Bob").size());
        assertTrue(graknGraph.getResourcesByValue("Bob").contains(c1));
        assertTrue(graknGraph.getResourcesByValue("Bob").contains(c2));
        assertEquals(c1, c3);
        assertNotEquals(c1, c2);
    }

    @Test
    public void getTypes(){
        String entityTypeName = "My Entity Type";
        String relationTypeName = "My Relation Type";
        String roleTypeName = "My Role Type";
        String resourceTypeName = "My Resource Type";
        String ruleTypeName = "My Rule Type";

        assertNull(graknGraph.getEntityType(entityTypeName));
        assertNull(graknGraph.getRelationType(relationTypeName));
        assertNull(graknGraph.getRoleType(roleTypeName));
        assertNull(graknGraph.getResourceType(resourceTypeName));
        assertNull(graknGraph.getRuleType(ruleTypeName));

        EntityType entityType = graknGraph.putEntityType(entityTypeName);
        RelationType relationType = graknGraph.putRelationType(relationTypeName);
        RoleType roleType = graknGraph.putRoleType(roleTypeName);
        ResourceType resourceType = graknGraph.putResourceType(resourceTypeName, ResourceType.DataType.STRING);
        RuleType ruleType = graknGraph.putRuleType(ruleTypeName);

        assertEquals(entityType, graknGraph.getEntityType(entityTypeName));
        assertEquals(relationType, graknGraph.getRelationType(relationTypeName));
        assertEquals(roleType, graknGraph.getRoleType(roleTypeName));
        assertEquals(resourceType, graknGraph.getResourceType(resourceTypeName));
        assertEquals(ruleType, graknGraph.getRuleType(ruleTypeName));
    }

    @Test
    public void testGetResource(){
        ResourceType<String> type = graknGraph.putResourceType("Type", ResourceType.DataType.STRING);
        ResourceType<String> type2 = graknGraph.putResourceType("Type 2", ResourceType.DataType.STRING);
        Resource c2 = type.putResource("1");
        assertEquals(c2, graknGraph.getResourcesByValue("1").iterator().next());
        assertEquals(1, graknGraph.getResourcesByValue("1").size());
        assertEquals(c2, type.getResource("1"));
        assertNull(type2.getResource("1"));
    }

    @Test
    public void testMetaOntologyInitialisation(){
        Type metaConcept = graknGraph.getMetaConcept();
        RuleType metaRuleType = graknGraph.getMetaRuleType();

        assertEquals(8, metaConcept.subTypes().size());
        assertTrue("Entity Type missing from meta ontology",
                metaConcept.subTypes().contains(graknGraph.getMetaEntityType()));
        assertTrue("Relation Type missing from meta ontology",
                metaConcept.subTypes().contains(graknGraph.getMetaRelationType()));
        assertTrue("Role Type missing from meta ontology",
                metaConcept.subTypes().contains(graknGraph.getMetaRoleType()));
        assertTrue("Resource Type missing from meta ontology",
                metaConcept.subTypes().contains(graknGraph.getMetaResourceType()));
        assertTrue("Rule Type missing from meta ontology",
                metaConcept.subTypes().contains(metaRuleType));

        assertEquals(3, metaRuleType.subTypes().size());
        assertTrue("Inference Rule Type missing from meta ontology",
                metaRuleType.subTypes().contains(graknGraph.getMetaRuleInference()));
        assertTrue("Constraint Rule Type missing from meta ontology",
                metaRuleType.subTypes().contains(graknGraph.getMetaRuleConstraint()));
    }

    @Test
    public void testTypeLinksToMetaOntology(){
        assertEquals(graknGraph.getMetaEntityType(),
                graknGraph.putEntityType("My Entity Type").superType());
        assertEquals(graknGraph.getMetaRelationType(),
                graknGraph.putRelationType("My Relation Type").superType());
        assertEquals(graknGraph.getMetaRoleType(),
                graknGraph.putRoleType("My Role Type").superType());
        assertEquals(graknGraph.getMetaResourceType(),
                graknGraph.putResourceType("My Resource Type", ResourceType.DataType.STRING).superType());
        assertEquals(graknGraph.getMetaRuleType(),
                graknGraph.putRuleType("My Rule Type").superType());
    }

    @Test
    public void testBuildConceptFromVertex(){
        EntityTypeImpl sampleEntityType = (EntityTypeImpl) graknGraph.putEntityType("Sample Entity Type");
        EntityType sampleEntityType2 = graknGraph.admin().buildConcept(sampleEntityType.getVertex());
        assertEquals(sampleEntityType, sampleEntityType2);
    }

    @Test
    public void testGetInstancesFromMeta(){
        EntityType sampleEntityType = graknGraph.putEntityType("Sample Entity Type");
        RelationType sampleRelationType = graknGraph.putRelationType("Sample Relation Type");
        RoleType sampleRoleType = graknGraph.putRoleType("Sample Role Type");

        Collection<? extends Concept> instances = graknGraph.getMetaConcept().instances();
        Collection<? extends Type> subTypes = graknGraph.getMetaConcept().subTypes();

        assertFalse(instances.contains(graknGraph.getMetaEntityType()));
        assertFalse(instances.contains(graknGraph.getMetaRelationType()));
        assertFalse(instances.contains(graknGraph.getMetaResourceType()));
        assertFalse(instances.contains(graknGraph.getMetaRoleType()));
        assertFalse(instances.contains(graknGraph.getMetaRuleType()));

        assertTrue(subTypes.contains(sampleEntityType));
        assertTrue(subTypes.contains(sampleRelationType));
        assertTrue(subTypes.contains(sampleRoleType));
    }

    @Test
    public void testSimpleGraqlQuery(){
        TypeName entityType = Schema.MetaSchema.ENTITY.getName();
        EntityType type1 = graknGraph.putEntityType("Concept Type ");
        EntityType type2 = graknGraph.putEntityType("Concept Type 1");

        List<Map<String, Concept>> results = graknGraph.graql().match(var("x").sub(entityType.getValue())).execute();

        boolean found = results.stream().map(Map::values).anyMatch(concepts -> concepts.stream().anyMatch(concept -> concept.equals(type1)));
        assertTrue(found);

        found = results.stream().map(Map::values).anyMatch(concepts -> concepts.stream().anyMatch(concept -> concept.equals(type2)));
        assertTrue(found);
    }

    @Test
    public void testImplicitFiltering(){
        //Build Implicit structures
        EntityType type = graknGraph.putEntityType("Concept Type ");
        ResourceType resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);
        type.hasResource(resourceType);

        assertFalse(graknGraph.implicitConceptsVisible());

        //Check nothing is revealed when returning result sets
        assertEquals(0, type.playsRoles().size());
        assertEquals(0, resourceType.playsRoles().size());
        assertEquals(1, graknGraph.getMetaRelationType().subTypes().size());
        assertEquals(1, graknGraph.getMetaRoleType().subTypes().size());

        //Check things are still returned when explicitly asking for them
        assertNotNull(graknGraph.getType(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceType.getName())));
        assertNotNull(graknGraph.getType(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceType.getName())));
        assertNotNull(graknGraph.getType(Schema.Resource.HAS_RESOURCE.getName(resourceType.getName())));

        //Switch on flag
        graknGraph.showImplicitConcepts(true);
        assertTrue(graknGraph.implicitConceptsVisible());

        //Now check the result sets again
        assertEquals(1, type.playsRoles().size());
        assertEquals(1, resourceType.playsRoles().size());
        assertEquals(2, graknGraph.getMetaRelationType().subTypes().size());
        assertEquals(3, graknGraph.getMetaRoleType().subTypes().size());
    }

    @Test
    public void testGraphIsClosed() throws ExecutionException, InterruptedException {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        GraknGraph graph = Grakn.factory(Grakn.IN_MEMORY, "testing").getGraph();

        final boolean[] errorThrown = {false};

        Future future = pool.submit(() -> {
            try{
                graph.putEntityType("A Thing");
            } catch (GraphRuntimeException e){
                if(e.getMessage().equals(ErrorMessage.GRAPH_CLOSED.getMessage(graph.getKeyspace()))){
                    errorThrown[0] = true;
                }
            }
        });

        future.get();

        assertTrue("Error not thrown when graph is closed in another thread", errorThrown[0]);
    }

    @Test
    public void testCloseAndReOpenGraph() throws GraknValidationException {
        GraknGraph graph = Grakn.factory(Grakn.IN_MEMORY, "testing").getGraph();
        graph.close();

        boolean errorThrown = false;
        try{
            graph.putEntityType("A Thing");
        } catch (GraphRuntimeException e){
            if(e.getMessage().equals(ErrorMessage.GRAPH_PERMANENTLY_CLOSED.getMessage(graph.getKeyspace()))){
                errorThrown = true;
            }
        }
        assertTrue("Graph not correctly closed", errorThrown);

        graph = Grakn.factory(Grakn.IN_MEMORY, "testing").getGraph();
        graph.putEntityType("A Thing");
    }

    @Test
    public void checkThatMainCentralCacheIsNotAffectedByTransactionModifications() throws GraknValidationException, ExecutionException, InterruptedException {
        //Check Central cache is empty
        assertTrue(graknGraph.getCachedOntology().asMap().isEmpty());

        RoleType r1 = graknGraph.putRoleType("r1");
        RoleType r2 = graknGraph.putRoleType("r2");
        EntityType e1 = graknGraph.putEntityType("e1").playsRole(r1).playsRole(r2);
        RelationType rel1 = graknGraph.putRelationType("rel1").hasRole(r1).hasRole(r2);

        //Purge the above concepts into the main cache
        graknGraph.commit();

        //Check cache is in good order
        assertThat(graknGraph.getCachedOntology().asMap().values(), containsInAnyOrder(r1, r2, e1, rel1,
                graknGraph.getMetaConcept(), graknGraph.getMetaEntityType(),
                graknGraph.getMetaRelationType(), graknGraph.getMetaRoleType()));

        assertThat(e1.playsRoles(), containsInAnyOrder(r1, r2));

        ExecutorService pool = Executors.newSingleThreadExecutor();
        //Mutate Ontology in a separate thread
        pool.submit(() -> {
            GraknGraph innerGraph = Grakn.factory(Grakn.IN_MEMORY, graknGraph.getKeyspace()).getGraph();
            EntityType entityType = innerGraph.getEntityType("e1");
            RoleType role = innerGraph.getRoleType("r1");
            entityType.deletePlaysRole(role);
        }).get();

        //Check the above mutation did not affect central repo
        Type foundE1 = graknGraph.getCachedOntology().asMap().get(e1.getName());
        assertTrue("Main cache was affected by transaction", foundE1.playsRoles().contains(r1));
    }

    @Test
    public void checkComplexOntologyCanLoad() throws GraknValidationException {
        graknGraph.graql().parse("insert\n" +
                "user-interaction sub relation is-abstract;\n" +
                "qa sub user-interaction\n" +
                "    has-resource helpful-votes\n" +
                "    has-resource unhelpful-votes\n" +
                "    has-role asked-question\n" +
                "    has-role given-answer\n" +
                "    has-role item;\n" +
                "product-review sub user-interaction\n" +
                "    has-resource rating\n" +
                "    has-role reviewer\n" +
                "    has-role feedback\n" +
                "    has-role item;\n" +
                "comment sub entity\n" +
                "    has-resource text\n" +
                "    has-resource time;\n" +
                "time sub resource datatype long;\n" +
                "question sub comment\n" +
                "    plays-role asked-question; \n" +
                "yes-no sub question;\n" +
                "open sub question;\n" +
                "answer sub comment\n" +
                "    plays-role given-answer\n" +
                "    has-resource answer-type;\n" +
                "answer-type sub resource datatype string;\n" +
                "review sub comment\n" +
                "    plays-role feedback\n" +
                "    has-resource summary;\n" +
                "summary sub text;\n" +
                "text sub resource datatype string;\n" +
                "rating sub resource datatype double;\n" +
                "helpful-votes sub resource datatype long;\n" +
                "unhelpful-votes sub resource datatype long;\n" +
                "ID sub resource is-abstract datatype string;\n" +
                "product sub entity\n" +
                "    has-resource asin\n" +
                "    has-resource price\n" +
                "    has-resource image-url\n" +
                "    has-resource brand\n" +
                "    has-resource name\n" +
                "    has-resource text\n" +
                "    plays-role item\n" +
                "    plays-role recommended;\n" +
                "asin sub ID;\n" +
                "image-url sub resource datatype string;\n" +
                "brand sub name;\n" +
                "price sub resource datatype double;\n" +
                "category sub entity\n" +
                "    has-resource name\n" +
                "    plays-role subcategory\n" +
                "    plays-role supercategory\n" +
                "    plays-role label\n" +
                "    plays-role item\n" +
                "    plays-role recommended;\n" +
                "name sub resource datatype string;\n" +
                "hierarchy sub relation\n" +
                "    has-role subcategory\n" +
                "    has-role supercategory;\n" +
                "category-assignment sub relation\n" +
                "    has-resource rank\n" +
                "    has-role item #product\n" +
                "    has-role label; #category \n" +
                "rank sub resource datatype long;\n" +
                "user sub entity\n" +
                "    has-resource uid\n" +
                "    has-resource username\n" +
                "    plays-role reviewer\n" +
                "    plays-role buyer;\n" +
                "uid sub ID;\n" +
                "username sub name;\n" +
                "completed-recommendation sub relation\n" +
                "    has-role successful-recommendation\n" +
                "    has-role buyer;\n" +
                "implied-recommendation sub relation\n" +
                "    has-role category-recommendation\n" +
                "    has-role product-recommendation;\n" +
                "recommendation sub relation is-abstract\n" +
                "    plays-role successful-recommendation\n" +
                "    plays-role product-recommendation;\n" +
                "co-categories sub relation\n" +
                "    plays-role category-recommendation\n" +
                "    has-role item\n" +
                "    has-role recommended;\n" +
                "also-viewed sub recommendation\n" +
                "    has-role item\n" +
                "    has-role recommended;\n" +
                "also-bought sub recommendation\n" +
                "    has-role item\n" +
                "    has-role recommended;\n" +
                "bought-together sub recommendation\n" +
                "    has-role item\n" +
                "    has-role recommended;\n" +
                "transaction sub relation\n" +
                "    has-role buyer\n" +
                "    has-role item;\n" +
                "asked-question sub role;\n" +
                "given-answer sub role;\n" +
                "item sub role;\n" +
                "feedback sub role;\n" +
                "reviewer sub role;\n" +
                "buyer sub role;\n" +
                "recommended sub role;\n" +
                "subcategory sub role;\n" +
                "supercategory sub role;\n" +
                "label sub role;\n" +
                "successful-recommendation sub role;\n" +
                "category-recommendation sub role;\n" +
                "product-recommendation sub role;").execute();

        graknGraph.commit();
    }
}
