package ai.grakn.graph.internal;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graph.internal.concept.EntityTypeImpl;
import ai.grakn.graph.internal.structure.Shard;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class GraknGraphTest extends GraphTestBase {

    @Test
    public void whenGettingConceptById_ReturnTheConcept(){
        EntityType entityType = graknGraph.putEntityType("test-name");
        assertEquals(entityType, graknGraph.getConcept(entityType.getId()));
    }

    @Test
    public void whenAttemptingToMutateViaTraversal_Throw(){
        expectedException.expect(VerificationException.class);
        expectedException.expectMessage("not read only");
        graknGraph.getTinkerTraversal().V().drop().iterate();
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
        String entityTypeLabel = "My Entity Type";
        String relationTypeLabel = "My Relation Type";
        String roleTypeLabel = "My Role Type";
        String resourceTypeLabel = "My Resource Type";
        String ruleTypeLabel = "My Rule Type";

        assertNull(graknGraph.getEntityType(entityTypeLabel));
        assertNull(graknGraph.getRelationType(relationTypeLabel));
        assertNull(graknGraph.getRole(roleTypeLabel));
        assertNull(graknGraph.getResourceType(resourceTypeLabel));
        assertNull(graknGraph.getRuleType(ruleTypeLabel));

        EntityType entityType = graknGraph.putEntityType(entityTypeLabel);
        RelationType relationType = graknGraph.putRelationType(relationTypeLabel);
        Role role = graknGraph.putRole(roleTypeLabel);
        ResourceType resourceType = graknGraph.putResourceType(resourceTypeLabel, ResourceType.DataType.STRING);
        RuleType ruleType = graknGraph.putRuleType(ruleTypeLabel);

        assertEquals(entityType, graknGraph.getEntityType(entityTypeLabel));
        assertEquals(relationType, graknGraph.getRelationType(relationTypeLabel));
        assertEquals(role, graknGraph.getRole(roleTypeLabel));
        assertEquals(resourceType, graknGraph.getResourceType(resourceTypeLabel));
        assertEquals(ruleType, graknGraph.getRuleType(ruleTypeLabel));
    }

    @Test
    public void whenGettingSubTypesFromRootMeta_IncludeAllTypes(){
        EntityType sampleEntityType = graknGraph.putEntityType("Sample Entity Type");
        RelationType sampleRelationType = graknGraph.putRelationType("Sample Relation Type");

        assertThat(graknGraph.admin().getMetaConcept().subs(), containsInAnyOrder(
                graknGraph.admin().getMetaConcept(),
                graknGraph.admin().getMetaRelationType(),
                graknGraph.admin().getMetaEntityType(),
                graknGraph.admin().getMetaRuleType(),
                graknGraph.admin().getMetaResourceType(),
                graknGraph.admin().getMetaRuleConstraint(),
                graknGraph.admin().getMetaRuleInference(),
                sampleEntityType,
                sampleRelationType
        ));
    }

    @Test
    public void whenClosingReadOnlyGraph_EnsureTypesAreCached(){
        assertCacheOnlyContainsMetaTypes();
        //noinspection ResultOfMethodCallIgnored
        graknGraph.getMetaConcept().subs(); //This loads some types into transaction cache
        graknGraph.abort();
        assertCacheOnlyContainsMetaTypes(); //Ensure central cache is empty

        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.READ);

        Set<OntologyConcept> finalTypes = new HashSet<>();
        finalTypes.addAll(graknGraph.getMetaConcept().subs());
        finalTypes.add(graknGraph.admin().getMetaRole());

        graknGraph.abort();

        for (OntologyConcept type : graknGraph.getGraphCache().getCachedTypes().values()) {
            assertTrue("Type [" + type + "] is missing from central cache after closing read only graph", finalTypes.contains(type));
        }
    }
    private void assertCacheOnlyContainsMetaTypes(){
        Set<Label> metas = Stream.of(Schema.MetaSchema.values()).map(Schema.MetaSchema::getLabel).collect(Collectors.toSet());
        graknGraph.getGraphCache().getCachedTypes().keySet().forEach(cachedLabel -> assertTrue("Type [" + cachedLabel + "] is missing from central cache", metas.contains(cachedLabel)));
    }

    @Test
    public void whenBuildingAConceptFromAVertex_ReturnConcept(){
        EntityTypeImpl et = (EntityTypeImpl) graknGraph.putEntityType("Sample Entity Type");
        assertEquals(et, graknGraph.factory().buildConcept(et.vertex()));
    }

    @Test
    public void whenPassingGraphToAnotherThreadWithoutOpening_Throw() throws ExecutionException, InterruptedException {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        GraknGraph graph = Grakn.session(Grakn.IN_MEMORY, "testing").open(GraknTxType.WRITE);

        expectedException.expectCause(IsInstanceOf.instanceOf(GraphOperationException.class));
        expectedException.expectMessage(GraphOperationException.transactionClosed(graph, null).getMessage());

        Future future = pool.submit(() -> {
            graph.putEntityType("A Thing");
        });
        future.get();
    }

    @Test
    public void attemptingToUseClosedGraphFailingThenOpeningGraph_EnsureGraphIsUsable() throws InvalidGraphException {
        GraknGraph graph = Grakn.session(Grakn.IN_MEMORY, "testing-again").open(GraknTxType.WRITE);
        graph.close();

        boolean errorThrown = false;
        try{
            graph.putEntityType("A Thing");
        } catch (GraphOperationException e){
            if(e.getMessage().equals(ErrorMessage.GRAPH_CLOSED_ON_ACTION.getMessage("closed", graph.getKeyspace()))){
                errorThrown = true;
            }
        }
        assertTrue("Graph not correctly closed", errorThrown);

        graph = Grakn.session(Grakn.IN_MEMORY, "testing-again").open(GraknTxType.WRITE);
        graph.putEntityType("A Thing");
    }

    @Test
    public void checkThatMainCentralCacheIsNotAffectedByTransactionModifications() throws InvalidGraphException, ExecutionException, InterruptedException {
        //Check Central cache is empty
        assertCacheOnlyContainsMetaTypes();

        Role r1 = graknGraph.putRole("r1");
        Role r2 = graknGraph.putRole("r2");
        EntityType e1 = graknGraph.putEntityType("e1").plays(r1).plays(r2);
        RelationType rel1 = graknGraph.putRelationType("rel1").relates(r1).relates(r2);

        //Purge the above concepts into the main cache
        graknGraph.commit();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);

        //Check cache is in good order
        Collection<OntologyConcept> cachedValues = graknGraph.getGraphCache().getCachedTypes().values();
        assertTrue("Type [" + r1 + "] was not cached", cachedValues.contains(r1));
        assertTrue("Type [" + r2 + "] was not cached", cachedValues.contains(r2));
        assertTrue("Type [" + e1 + "] was not cached", cachedValues.contains(e1));
        assertTrue("Type [" + rel1 + "] was not cached", cachedValues.contains(rel1));

        assertThat(e1.plays(), containsInAnyOrder(r1, r2));

        ExecutorService pool = Executors.newSingleThreadExecutor();
        //Mutate Ontology in a separate thread
        pool.submit(() -> {
            GraknGraph innerGraph = Grakn.session(Grakn.IN_MEMORY, graknGraph.getKeyspace()).open(GraknTxType.WRITE);
            EntityType entityType = innerGraph.getEntityType("e1");
            Role role = innerGraph.getRole("r1");
            entityType.deletePlays(role);
        }).get();

        //Check the above mutation did not affect central repo
        OntologyConcept foundE1 = graknGraph.getGraphCache().getCachedTypes().get(e1.getLabel());
        assertTrue("Main cache was affected by transaction", ((Type) foundE1).plays().contains(r1));
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
        failMutation(graknGraph, () -> graknGraph.putRole(roleType1));
        failMutation(graknGraph, () -> graknGraph.putRelationType(relationType1));

        //Pass some mutations
        graknGraph.close();
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, keyspace).open(GraknTxType.WRITE);
        EntityType entityT = graknGraph.putEntityType(entityType);
        entityT.addEntity();
        Role roleT1 = graknGraph.putRole(roleType1);
        Role roleT2 = graknGraph.putRole(roleType2);
        RelationType relationT1 = graknGraph.putRelationType(relationType1).relates(roleT1);
        RelationType relationT2 = graknGraph.putRelationType(relationType2).relates(roleT2);
        ResourceType<String> resourceT = graknGraph.putResourceType(resourceType, ResourceType.DataType.STRING);
        graknGraph.commit();

        //Fail some mutations again
        graknGraph = (AbstractGraknGraph<?>) Grakn.session(Grakn.IN_MEMORY, keyspace).open(GraknTxType.READ);
        failMutation(graknGraph, entityT::addEntity);
        failMutation(graknGraph, () -> resourceT.putResource("A resource"));
        failMutation(graknGraph, () -> graknGraph.putEntityType(entityType));
        failMutation(graknGraph, () -> entityT.plays(roleT1));
        failMutation(graknGraph, () -> relationT1.relates(roleT2));
        failMutation(graknGraph, () -> relationT2.relates(roleT1));
    }
    private void failMutation(GraknGraph graph, Runnable mutator){
        int vertexCount = graph.admin().getTinkerTraversal().V().toList().size();
        int eddgeCount = graph.admin().getTinkerTraversal().E().toList().size();

        Exception caughtException = null;
        try{
            mutator.run();
        } catch (Exception e){
            caughtException = e;
        }

        assertNotNull("No exception thrown when attempting to mutate a read only graph", caughtException);
        assertThat(caughtException, instanceOf(GraphOperationException.class));
        assertEquals(caughtException.getMessage(), ErrorMessage.TRANSACTION_READ_ONLY.getMessage(graph.getKeyspace()));
        assertEquals("A concept was added/removed using a read only graph", vertexCount, graph.admin().getTinkerTraversal().V().toList().size());
        assertEquals("An edge was added/removed using a read only graph", eddgeCount, graph.admin().getTinkerTraversal().E().toList().size());
    }

    @Test
    public void whenOpeningDifferentTypesOfGraphsOnTheSameThread_Throw(){
        String keyspace = "akeyspacewithkeys";
        GraknSession session = Grakn.session(Grakn.IN_MEMORY, keyspace);

        GraknGraph graph = session.open(GraknTxType.READ);
        failAtOpeningGraph(session, GraknTxType.WRITE, keyspace);
        failAtOpeningGraph(session, GraknTxType.BATCH, keyspace);
        graph.close();

        //noinspection ResultOfMethodCallIgnored
        session.open(GraknTxType.BATCH);
        failAtOpeningGraph(session, GraknTxType.WRITE, keyspace);
        failAtOpeningGraph(session, GraknTxType.READ, keyspace);
    }

    private void failAtOpeningGraph(GraknSession session, GraknTxType txType, String keyspace){
        Exception exception = null;
        try{
            //noinspection ResultOfMethodCallIgnored
            session.open(txType);
        } catch (GraphOperationException e){
            exception = e;
        }
        assertNotNull(exception);
        assertThat(exception, instanceOf(GraphOperationException.class));
        assertEquals(exception.getMessage(), ErrorMessage.TRANSACTION_ALREADY_OPEN.getMessage(keyspace));
    }

    @Test
    public void whenShardingSuperNode_EnsureNewInstancesGoToNewShard(){
        EntityTypeImpl entityType = (EntityTypeImpl) graknGraph.putEntityType("The Special Type");
        Shard s1 = entityType.currentShard();

        //Add 3 instances to first shard
        Entity s1_e1 = entityType.addEntity();
        Entity s1_e2 = entityType.addEntity();
        Entity s1_e3 = entityType.addEntity();
        graknGraph.admin().shard(entityType.getId());

        Shard s2 = entityType.currentShard();

        //Add 5 instances to second shard
        Entity s2_e1 = entityType.addEntity();
        Entity s2_e2 = entityType.addEntity();
        Entity s2_e3 = entityType.addEntity();
        Entity s2_e4 = entityType.addEntity();
        Entity s2_e5 = entityType.addEntity();

        graknGraph.admin().shard(entityType.getId());
        Shard s3 = entityType.currentShard();

        //Add 2 instances to 3rd shard
        Entity s3_e1 = entityType.addEntity();
        Entity s3_e2 = entityType.addEntity();

        //Check Type was sharded correctly
        assertThat(entityType.shards(), containsInAnyOrder(s1, s2, s3));

        //Check shards have correct instances
        assertThat(s1.links().collect(Collectors.toSet()), containsInAnyOrder(s1_e1, s1_e2, s1_e3));
        assertThat(s2.links().collect(Collectors.toSet()), containsInAnyOrder(s2_e1, s2_e2, s2_e3, s2_e4, s2_e5));
        assertThat(s3.links().collect(Collectors.toSet()), containsInAnyOrder(s3_e1, s3_e2));
    }

    @Test
    public void whenCreatingAValidOntologyInSeparateThreads_EnsureValidationRulesHold() throws ExecutionException, InterruptedException {
        GraknSession session = Grakn.session(Grakn.IN_MEMORY, "hi");

        ExecutorService executor = Executors.newCachedThreadPool();

        executor.submit(() -> {
            //Resources
            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {
                ResourceType<Long> int_ = graph.putResourceType("int", ResourceType.DataType.LONG);
                ResourceType<Long> foo = graph.putResourceType("foo", ResourceType.DataType.LONG).sup(int_);
                graph.putResourceType("bar", ResourceType.DataType.LONG).sup(int_);
                graph.putEntityType("FOO").resource(foo);

                graph.commit();
            }
        }).get();

        //Relation Which Has Resources
        try (GraknGraph graph = session.open(GraknTxType.WRITE)) {
            graph.putEntityType("BAR").resource(graph.getResourceType("bar"));
            graph.commit();
        }
    }
}

