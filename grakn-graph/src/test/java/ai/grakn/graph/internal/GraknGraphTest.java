package ai.grakn.graph.internal;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static ai.grakn.graql.Graql.var;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class GraknGraphTest extends GraphTestBase {

    @Test
    public void whenGettingConceptByRawID_ReturnTheConcept(){
        EntityType c1 = graknGraph.putEntityType("c1");
        Concept c2 = graknGraph.getConceptRawId(c1.getId());
        assertEquals(c1, c2);
    }

    @Test
    public void whenGettingConceptById_ReturnTheConcept(){
        EntityType entityType = graknGraph.putEntityType("test-name");
        assertEquals(entityType, graknGraph.getConcept(entityType.getId()));
    }

    @Test
    public void whenAttemptingToMutateViaTraversal_Throw(){
        expectedException.expect(VerificationException.class);
        expectedException.expectMessage("not read only");
        graknGraph.getTinkerTraversal().drop().iterate();
    }

    @Test
    public void whenGettingResourcesByValue_ReturnTheMatchingResources(){
        String targetValue = "Geralt";
        assertThat(graknGraph.getResourcesByValue(targetValue), is(empty()));

        ResourceType<String> t1 = graknGraph.putResourceType("Parent 1", ResourceType.DataType.STRING);
        ResourceType<String> t2 = graknGraph.putResourceType("Parent 2", ResourceType.DataType.STRING);

        Resource<String> r1 = t1.putResource(targetValue);
        Resource<String> r2 = t2.putResource(targetValue);
        t2.putResource("Dragon");

        assertThat(graknGraph.getResourcesByValue(targetValue), containsInAnyOrder(r1, r2));
    }

    @Test
    public void whenGettingTypesByName_ReturnTypes(){
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
    public void whenGettingSubTypesFromRootMeta_IncludeAllTypes(){
        EntityType sampleEntityType = graknGraph.putEntityType("Sample Entity Type");
        RelationType sampleRelationType = graknGraph.putRelationType("Sample Relation Type");
        RoleType sampleRoleType = graknGraph.putRoleType("Sample Role Type");

        assertThat(graknGraph.admin().getMetaConcept().subTypes(), containsInAnyOrder(
                graknGraph.admin().getMetaConcept(),
                graknGraph.admin().getMetaRoleType(),
                graknGraph.admin().getMetaRelationType(),
                graknGraph.admin().getMetaEntityType(),
                graknGraph.admin().getMetaRuleType(),
                graknGraph.admin().getMetaResourceType(),
                graknGraph.admin().getMetaRuleConstraint(),
                graknGraph.admin().getMetaRuleInference(),
                sampleEntityType,
                sampleRelationType,
                sampleRoleType
        ));
    }

    @Test
    public void whenClosingReadOnlyGraph_EnsureTypesAreCached(){
        assertThat(graknGraph.getCachedOntology().asMap().keySet(), is(empty()));
        graknGraph.getMetaConcept().subTypes(); //This loads some types into transaction cache
        graknGraph.abort();
        assertThat(graknGraph.getCachedOntology().asMap().values(), is(empty())); //Ensure central cache is empty

        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.READ);
        Collection<? extends Type> types = graknGraph.getMetaConcept().subTypes();
        graknGraph.abort();

        for (Type type : graknGraph.getCachedOntology().asMap().values()) {
            assertTrue("Type [" + type + "] is missing from central cache after closing read only graph", types.contains(type));
        }
    }

    @Test
    public void whenBuildingAConceptFromAVertex_ReturnConcept(){
        EntityTypeImpl et = (EntityTypeImpl) graknGraph.putEntityType("Sample Entity Type");
        assertEquals(et, graknGraph.admin().buildConcept(et.getVertex()));
    }

    @Test
    public void whenExecutingGraqlTraversalFromGraph_ReturnExpectedResults(){
        EntityType type = graknGraph.putEntityType("Concept Type");
        Entity entity = type.addEntity();

        Collection<Concept> results = graknGraph.graql().match(var("x").isa(type.getName().getValue())).
                execute().iterator().next().values();

        assertThat(results, containsInAnyOrder(entity));
    }

    @Test
    public void whenAllowingImplicitTypesToBeShow_ReturnImplicitTypes(){
        //Build Implicit structures
        EntityType type = graknGraph.putEntityType("Concept Type ");
        ResourceType resourceType = graknGraph.putResourceType("Resource Type", ResourceType.DataType.STRING);
        type.resource(resourceType);

        assertFalse(graknGraph.implicitConceptsVisible());

        //Meta Types
        RelationType relationType = graknGraph.admin().getMetaRelationType();
        RoleType roleType = graknGraph.admin().getMetaRoleType();

        //Check nothing is revealed when returning result sets
        assertThat(type.playsRoles(), is(empty()));
        assertThat(resourceType.playsRoles(), is(empty()));
        assertThat(graknGraph.getMetaRelationType().subTypes(), containsInAnyOrder(relationType));
        assertThat(graknGraph.getMetaRoleType().subTypes(), containsInAnyOrder(roleType));

        //Check things are still returned when explicitly asking for them
        RelationType has = graknGraph.getRelationType(Schema.ImplicitType.HAS_RESOURCE.getName(resourceType.getName()).getValue());
        RoleType hasOwner = graknGraph.getRoleType(Schema.ImplicitType.HAS_RESOURCE_OWNER.getName(resourceType.getName()).getValue());
        RoleType hasValue = graknGraph.getRoleType(Schema.ImplicitType.HAS_RESOURCE_VALUE.getName(resourceType.getName()).getValue());
        assertNotNull(hasOwner);
        assertNotNull(hasValue);
        assertNotNull(has);

        //Switch on flag
        graknGraph.showImplicitConcepts(true);
        assertTrue(graknGraph.implicitConceptsVisible());

        //Now check the result sets again
        assertThat(graknGraph.getMetaRelationType().subTypes(), containsInAnyOrder(relationType, has));
        assertThat(graknGraph.getMetaRoleType().subTypes(), containsInAnyOrder(roleType, hasOwner, hasValue));
        assertThat(type.playsRoles(), containsInAnyOrder(hasOwner));
        assertThat(resourceType.playsRoles(), containsInAnyOrder(hasValue));
    }

    @Test
    public void whenPassingGraphToAnotherThreadWithoutOpening_Throw() throws ExecutionException, InterruptedException {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        GraknGraph graph = Grakn.session(Grakn.IN_MEMORY, "testing").open(GraknTxType.WRITE);

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
    public void attemptingToUseClosedGraphFailingThenOpeningGraph_EnsureGraphIsUsable() throws GraknValidationException {
        GraknGraph graph = Grakn.session(Grakn.IN_MEMORY, "testing-again").open(GraknTxType.WRITE);
        graph.close();

        boolean errorThrown = false;
        try{
            graph.putEntityType("A Thing");
        } catch (GraphRuntimeException e){
            if(e.getMessage().equals(ErrorMessage.GRAPH_CLOSED_ON_ACTION.getMessage("closed", graph.getKeyspace()))){
                errorThrown = true;
            }
        }
        assertTrue("Graph not correctly closed", errorThrown);

        graph = Grakn.session(Grakn.IN_MEMORY, "testing-again").open(GraknTxType.WRITE);
        graph.putEntityType("A Thing");
    }

    @Test
    public void checkThatMainCentralCacheIsNotAffectedByTransactionModifications() throws GraknValidationException, ExecutionException, InterruptedException {
        //Check Central cache is empty
        assertTrue(graknGraph.getCachedOntology().asMap().isEmpty());

        RoleType r1 = graknGraph.putRoleType("r1");
        RoleType r2 = graknGraph.putRoleType("r2");
        EntityType e1 = graknGraph.putEntityType("e1").playsRole(r1).playsRole(r2);
        RelationType rel1 = graknGraph.putRelationType("rel1").relates(r1).relates(r2);

        //Purge the above concepts into the main cache
        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        //Check cache is in good order
        assertThat(graknGraph.getCachedOntology().asMap().values(), containsInAnyOrder(r1, r2, e1, rel1,
                graknGraph.getMetaConcept(), graknGraph.getMetaEntityType(),
                graknGraph.getMetaRelationType(), graknGraph.getMetaRoleType()));

        assertThat(e1.playsRoles(), containsInAnyOrder(r1, r2));

        ExecutorService pool = Executors.newSingleThreadExecutor();
        //Mutate Ontology in a separate thread
        pool.submit(() -> {
            GraknGraph innerGraph = Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);
            EntityType entityType = innerGraph.getEntityType("e1");
            RoleType role = innerGraph.getRoleType("r1");
            entityType.deletePlaysRole(role);
        }).get();

        //Check the above mutation did not affect central repo
        Type foundE1 = graknGraph.getCachedOntology().asMap().get(e1.getName());
        assertTrue("Main cache was affected by transaction", foundE1.playsRoles().contains(r1));
    }

    @Test
    public void whenClosingAGraphWhichWasJustCommitted_DoNothing(){
        graknGraph.commit();
        assertTrue("Graph is still open after commit", graknGraph.isClosed());
        graknGraph.close();
        assertTrue("Graph is somehow open after close", graknGraph.isClosed());
    }

    @Test
    public void whenCommittingAGraphWhichWasJustCommitted_DoNothing(){
        graknGraph.commit();
        assertTrue("Graph is still open after commit", graknGraph.isClosed());
        graknGraph.commit();
        assertTrue("Graph is somehow open after 2nd commit", graknGraph.isClosed());
    }

    @Test
    public void whenAttemptingToMutateReadOnlyGraph_Throw(){
        String keyspace = "my-read-only-graph";
        String entityType = "My Entity Type";
        String roleType1 = "My Role Type 1";
        String roleType2 = "My Role Type 2";
        String relationType1 = "My Relation Type 1";
        String relationType2 = "My Relation Type 2";
        String resourceType = "My Resource Type";

        //Fail Some Mutations
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, keyspace).open(GraknTxType.READ);
        failMutation(graknGraph, () -> graknGraph.putEntityType(entityType));
        failMutation(graknGraph, () -> graknGraph.putRoleType(roleType1));
        failMutation(graknGraph, () -> graknGraph.putRelationType(relationType1));

        //Pass some mutations
        graknGraph.close();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, keyspace).open(GraknTxType.WRITE);
        EntityType entityT = graknGraph.putEntityType(entityType);
        entityT.addEntity();
        RoleType roleT1 = graknGraph.putRoleType(roleType1);
        RoleType roleT2 = graknGraph.putRoleType(roleType2);
        RelationType relationT1 = graknGraph.putRelationType(relationType1).relates(roleT1);
        RelationType relationT2 = graknGraph.putRelationType(relationType2).relates(roleT2);
        ResourceType<String> resourceT = graknGraph.putResourceType(resourceType, ResourceType.DataType.STRING);
        graknGraph.commit();

        //Fail some mutations again
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, keyspace).open(GraknTxType.READ);
        failMutation(graknGraph, entityT::addEntity);
        failMutation(graknGraph, () -> resourceT.putResource("A resource"));
        failMutation(graknGraph, () -> graknGraph.putEntityType(entityType));
        failMutation(graknGraph, () -> entityT.playsRole(roleT1));
        failMutation(graknGraph, () -> relationT1.relates(roleT2));
        failMutation(graknGraph, () -> relationT2.relates(roleT1));
    }
    private void failMutation(GraknGraph graph, Runnable mutator){
        int vertexCount = graph.admin().getTinkerTraversal().toList().size();
        int eddgeCount = graph.admin().getTinkerTraversal().bothE().toList().size();

        Exception caughtException = null;
        try{
            mutator.run();
        } catch (Exception e){
            caughtException = e;
        }

        assertNotNull("No exception thrown when attempting to mutate a read only graph", caughtException);
        assertThat(caughtException, instanceOf(GraphRuntimeException.class));
        assertEquals(caughtException.getMessage(), ErrorMessage.TRANSACTION_READ_ONLY.getMessage(graph.getKeyspace()));
        assertEquals("A concept was added/removed using a read only graph", vertexCount, graph.admin().getTinkerTraversal().toList().size());
        assertEquals("An edge was added/removed using a read only graph", eddgeCount, graph.admin().getTinkerTraversal().bothE().toList().size());
    }

    @Test
    public void whenOpeningDifferentTypesOfGraphsOnTheSameThread_Throw(){
        String keyspace = "akeyspacewithkeys";
        GraknSession session = Grakn.session(Grakn.IN_MEMORY, keyspace);

        GraknGraph graph = session.open(GraknTxType.READ);
        failAtOpeningGraph(session, GraknTxType.WRITE, keyspace);
        failAtOpeningGraph(session, GraknTxType.BATCH, keyspace);
        graph.close();

        session.open(GraknTxType.BATCH);
        failAtOpeningGraph(session, GraknTxType.WRITE, keyspace);
        failAtOpeningGraph(session, GraknTxType.READ, keyspace);
    }
    private void failAtOpeningGraph(GraknSession session, GraknTxType txType, String keyspace){
        Exception exception = null;
        try{
            session.open(txType);
        } catch (GraphRuntimeException e){
            exception = e;
        }
        assertNotNull(exception);
        assertThat(exception, instanceOf(GraphRuntimeException.class));
        assertEquals(exception.getMessage(), ErrorMessage.TRANSACTION_ALREADY_OPEN.getMessage(keyspace));
    }

    @Test
    public void checkComplexSampleOntologyCanLoad() throws GraknValidationException {
        graknGraph.graql().parse("insert\n" +
                "user-interaction sub relation is-abstract;\n" +
                "qa sub user-interaction\n" +
                "    has-resource helpful-votes\n" +
                "    has-resource unhelpful-votes\n" +
                "    relates asked-question\n" +
                "    relates given-answer\n" +
                "    relates item;\n" +
                "product-review sub user-interaction\n" +
                "    has-resource rating\n" +
                "    relates reviewer\n" +
                "    relates feedback\n" +
                "    relates item;\n" +
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
                "    relates subcategory\n" +
                "    relates supercategory;\n" +
                "category-assignment sub relation\n" +
                "    has-resource rank\n" +
                "    relates item #product\n" +
                "    relates label; #category \n" +
                "rank sub resource datatype long;\n" +
                "user sub entity\n" +
                "    has-resource uid\n" +
                "    has-resource username\n" +
                "    plays-role reviewer\n" +
                "    plays-role buyer;\n" +
                "uid sub ID;\n" +
                "username sub name;\n" +
                "completed-recommendation sub relation\n" +
                "    relates successful-recommendation\n" +
                "    relates buyer;\n" +
                "implied-recommendation sub relation\n" +
                "    relates category-recommendation\n" +
                "    relates product-recommendation;\n" +
                "recommendation sub relation is-abstract\n" +
                "    plays-role successful-recommendation\n" +
                "    plays-role product-recommendation;\n" +
                "co-categories sub relation\n" +
                "    plays-role category-recommendation\n" +
                "    relates item\n" +
                "    relates recommended;\n" +
                "also-viewed sub recommendation\n" +
                "    relates item\n" +
                "    relates recommended;\n" +
                "also-bought sub recommendation\n" +
                "    relates item\n" +
                "    relates recommended;\n" +
                "bought-together sub recommendation\n" +
                "    relates item\n" +
                "    relates recommended;\n" +
                "transaction sub relation\n" +
                "    relates buyer\n" +
                "    relates item;\n" +
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
