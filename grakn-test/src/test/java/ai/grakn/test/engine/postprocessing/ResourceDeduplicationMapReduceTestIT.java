package ai.grakn.test.engine.postprocessing;

import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.engine.postprocessing.ResourceDeduplicationTask;
import ai.grakn.test.EngineContext;
import ai.grakn.util.Schema;
import mjson.Json;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.function.Consumer;
import java.util.function.Function;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static ai.grakn.test.engine.postprocessing.PostProcessingTestUtils.checkUnique;
import static ai.grakn.test.engine.postprocessing.PostProcessingTestUtils.createDuplicateResource;
import static ai.grakn.test.engine.postprocessing.PostProcessingTestUtils.indexOf;
import static org.junit.Assume.assumeTrue;

public class ResourceDeduplicationMapReduceTestIT {

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    static GraknSession factory;
    static ResourceType<String> stringResource = null;
    static ResourceType<Long> longResource = null;
    static ResourceType<Double> doubleResource = null;
    static ResourceType<Integer> integerResource = null;
    static ResourceType<Boolean> booleanResource = null;
    static ResourceType<Float> floatResource = null;
    
    static EntityType thing, idea;
    static RelationType nearby, related;
    static RoleType near1, near2, near3;
    static RoleType related1, related2, related3, related4, related5;
    
    static void transact(Consumer<GraknGraph> action) {
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            action.accept(graph);
            graph.commit();
        }
    }

    static <T> T transact(Function<GraknGraph, T> action) {
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            T result = action.apply(graph);
            graph.commit();
            return result;
        }
    }
    
    @BeforeClass
    public static void onlyRunOnTinker(){
        assumeTrue(usingTinker());
    }

    private String keyspace() {
        try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
            return graph.getKeyspace();
        }        
    }
    
    private void miniOntology(GraknGraph graph) {
        stringResource = graph.putResourceType("StringResource", ResourceType.DataType.STRING);
        longResource = graph.putResourceType("LongResource", ResourceType.DataType.LONG);
        doubleResource = graph.putResourceType("DoubleResource", ResourceType.DataType.DOUBLE);
        integerResource = graph.putResourceType("IntegerResource", ResourceType.DataType.INTEGER);
        booleanResource = graph.putResourceType("BooleanResource", ResourceType.DataType.BOOLEAN);
        floatResource = graph.putResourceType("FloatResource", ResourceType.DataType.FLOAT);
        
        thing = graph.putEntityType("thing");
        idea = graph.putEntityType("idea");
        nearby = graph.putRelationType("nearby");
        near1 = graph.putRoleType("near1");
        near2 = graph.putRoleType("near2");
        near3 = graph.putRoleType("near3");        
        nearby.relates(near1).relates(near2).relates(near3);
        nearby.resource(stringResource).resource(longResource).resource(integerResource)
              .resource(booleanResource).resource(floatResource).resource(doubleResource);
        related = graph.putRelationType("related");
        related1 = graph.putRoleType("related1");
        related2 = graph.putRoleType("related2");
        related3 = graph.putRoleType("related3");
        related4 = graph.putRoleType("related4");
        related5 = graph.putRoleType("related5");
        related.relates(related1).relates(related2).relates(related3).relates(related4).relates(related5);
        thing.resource(stringResource);
        thing.resource(longResource);
        thing.resource(integerResource);
        thing.resource(booleanResource);
        thing.resource(floatResource);
        thing.resource(doubleResource);
        idea.resource(stringResource);
        idea.resource(longResource);
        idea.resource(integerResource);
        idea.resource(booleanResource);
        idea.resource(floatResource);
        idea.resource(doubleResource);
        related.resource(stringResource);
        related.resource(longResource);
        related.resource(integerResource);
        related.resource(booleanResource);
        related.resource(floatResource);
        related.resource(doubleResource);
        thing.plays(near1).plays(near2).plays(near3).plays(related1)
            .plays(related2).plays(related3).plays(related4).plays(related5);
        idea.plays(related1).plays(related2).plays(related3).plays(related4).plays(related5);
        stringResource.plays(related1).plays(related2).plays(related3)
                .plays(related4).plays(related5);
        floatResource.plays(related1).plays(related2);
    }
    
    @Before
    public void initWithEntitiesAndRelations() {
        try {
            factory = engine.factoryWithNewKeyspace();
        }
        catch (Throwable t) {
            t.printStackTrace(System.err);
            throw t;
        }        
        transact(graph -> { miniOntology(graph); });
    }
    
    @After
    public void emptyGraph() {
        GraknGraph graph = factory.open(GraknTxType.WRITE);
        graph.clear();
    }
    
    /**
     * Simple sanity test that nothing bad happens if the graph has no resources whatsoever.
     */
    @Test
    //@Ignore
    public void testNoResources() {
        ResourceDeduplicationTask task = new ResourceDeduplicationTask(); 
        task.start(checkpoint -> { throw new RuntimeException("No checkpoint expected."); }, 
                    Json.object(ResourceDeduplicationTask.KEYSPACE_CONFIG, keyspace()));
        Assert.assertEquals(new Long(0), task.totalElimintated());
    }
    
    /**
     * Test that the normal case with no duplicates on various resources doesn't screw things up.
     */
    @Test
    public void testNoDuplicates() {
        transact(graph ->  {
            Entity e1 = thing.addEntity();
            Entity e2 = thing.addEntity();
            Relation r1 = related.addRelation().addRolePlayer(related1, e1).addRolePlayer(related2, e2);
            e1.resource(stringResource.putResource("value_1"));            
            e1.resource(longResource.putResource(24234l));
            e2.resource(integerResource.putResource(42));
            r1.resource(booleanResource.putResource(true));
            r1.resource(floatResource.putResource(56.43f));
            r1.resource(doubleResource.putResource(2342.546));            
            graph.commit();
        });
        ResourceDeduplicationTask task = new ResourceDeduplicationTask(); 
        task.start(checkpoint -> { throw new RuntimeException("No checkpoint expected."); }, 
                    Json.object(ResourceDeduplicationTask.KEYSPACE_CONFIG, keyspace()));
        Assert.assertEquals(new Long(0), task.totalElimintated());        
    }
    
    /**
     * Test resource duplicates that are not attached to any entities or part of any
     * relationships.
     */
    @Test
    public void testManyUnattachedResources() {
        String stringIndex = transact(graph -> { 
            Resource<String> res = stringResource.putResource("value_dup");
            createDuplicateResource(graph, res);
            createDuplicateResource(graph, res);
            return indexOf(graph, res);
        } );
        String booleanIndex = transact(graph -> { 
            Resource<Boolean> res = booleanResource.putResource(true);
            createDuplicateResource(graph, res);
            createDuplicateResource(graph, res);
            return indexOf(graph, res);
        } );
        String doubleIndex = transact(graph -> { 
            Resource<Double> res = doubleResource.putResource(2.7182818284590452353602874713527);
            createDuplicateResource(graph, res);
            createDuplicateResource(graph, res);
            createDuplicateResource(graph, res);
            createDuplicateResource(graph, res);
            createDuplicateResource(graph, res);
            return indexOf(graph, res);
        } );
        transact(graph -> {
            Assert.assertFalse(checkUnique(graph, stringIndex));
            Assert.assertFalse(checkUnique(graph, booleanIndex));
            Assert.assertFalse(checkUnique(graph, doubleIndex));
        });
        ResourceDeduplicationTask task = new ResourceDeduplicationTask(); 
        task.start(checkpoint -> { throw new RuntimeException("No checkpoint expected."); }, 
                    Json.object(ResourceDeduplicationTask.KEYSPACE_CONFIG, keyspace()));        
        Assert.assertEquals(new Long(9), task.totalElimintated());
        transact(graph -> {
            Assert.assertTrue(checkUnique(graph, stringIndex));
            Assert.assertTrue(checkUnique(graph, booleanIndex));
            Assert.assertTrue(checkUnique(graph, doubleIndex));
        });
    }
    
    /**
     * Test when a few instances of the same resource get attached to the same entity.
     */
    @Test
    public void testDuplicatesOnSameEntity() {
        String resourceIndex = transact(graph -> {
           Entity something = thing.addEntity();
           Resource<String> res = stringResource.putResource("This is something!");
           something.resource(res);
           something.resource(createDuplicateResource(graph, res));
           return indexOf(graph, res);
        });        
        transact(graph -> {
            Assert.assertFalse(checkUnique(graph, resourceIndex));
        });        
        ResourceDeduplicationTask task = new ResourceDeduplicationTask(); 
        task.start(checkpoint -> { throw new RuntimeException("No checkpoint expected."); }, 
                    Json.object(ResourceDeduplicationTask.KEYSPACE_CONFIG, keyspace()));        
        Assert.assertEquals(new Long(1), task.totalElimintated());
        transact(graph -> {
            Assert.assertTrue(checkUnique(graph, resourceIndex));
        });        
    }

    @Test
    public void testDuplicatesOnDifferentEntity() {
        String resourceIndex = transact(graph -> {
           Entity something = thing.addEntity();
           Entity anotherthing = thing.addEntity();
           Entity onemorething = thing.addEntity();
           Resource<String> res = stringResource.putResource("This is something!");
           something.resource(res);
           something.resource(createDuplicateResource(graph, res));
           anotherthing.resource(createDuplicateResource(graph, res));
           onemorething.resource(createDuplicateResource(graph, res));
           return indexOf(graph, res);
        });        
        transact(graph -> {
            Assert.assertFalse(checkUnique(graph, resourceIndex));
        });        
        ResourceDeduplicationTask task = new ResourceDeduplicationTask(); 
        task.start(checkpoint -> { throw new RuntimeException("No checkpoint expected."); }, 
                    Json.object(ResourceDeduplicationTask.KEYSPACE_CONFIG, keyspace()));        
        Assert.assertEquals(new Long(3), task.totalElimintated());
        transact(graph -> {
            Assert.assertTrue(checkUnique(graph, resourceIndex));
            Resource<String> res = graph.admin().getConcept(Schema.ConceptProperty.INDEX, resourceIndex);
            Assert.assertEquals(3, res.ownerInstances().size());
        });        
    }
    
    /**
     * Test when duplicate resources are the components of the same relation. 
     */
    @Test
    @Ignore
    public void testDuplicatesWithinSameRelation() {
        String resourceIndex = transact(graph -> {
            Relation relation = related.addRelation();
            Resource<String> res = stringResource.putResource("This is something!");
            relation.addRolePlayer(related1, res);
            res = createDuplicateResource(graph, res);
            relation.addRolePlayer(related2, res);
            return indexOf(graph, res);
         });        
         transact(graph -> {
             Assert.assertFalse(checkUnique(graph, resourceIndex));
         });        
         ResourceDeduplicationTask task = new ResourceDeduplicationTask(); 
         task.start(checkpoint -> { throw new RuntimeException("No checkpoint expected."); }, 
                     Json.object(ResourceDeduplicationTask.KEYSPACE_CONFIG, keyspace()));        
         Assert.assertEquals(new Long(1), task.totalElimintated());
         transact(graph -> {
             Assert.assertTrue(checkUnique(graph, resourceIndex));
             Resource<String> res = graph.admin().getConcept(Schema.ConceptProperty.INDEX, resourceIndex);
             Assert.assertEquals(1, res.relations(related1).size());
             Assert.assertEquals(1, res.relations(related2).size());
         });        
    }

    /**
     * Test when duplicate resources are attached to different entities or relations, or are components
     * of different more complex (than 2-way) relationships.
     */
    @Test
    @Ignore
    public void testDuplicatesAcrossTheBoard() {
        String [] resourceKeys = transact(graph -> {
           Entity t1 = thing.addEntity(), 
                  t2 = thing.addEntity(),
                  t3 = thing.addEntity();
           Relation r1 = related.addRelation(),
                    r2 = related.addRelation();
           
           Resource<String> sres = stringResource.putResource("string-1");          
           t1.resource(sres);
           t3.resource(createDuplicateResource(graph, sres));
           r1.resource(createDuplicateResource(graph, sres));
           r2.resource(sres);
           r2.addRolePlayer(related1, sres);
           r1.addRolePlayer(related2, createDuplicateResource(graph, sres));
           
           createDuplicateResource(graph, sres); // free floating
           createDuplicateResource(graph, sres); // another free floating
           
           Resource<Float> fres = floatResource.putResource(1.69f);
           r1.resource(fres);
           r2.resource(createDuplicateResource(graph, fres));
           t2.resource(createDuplicateResource(graph, fres));
           t3.resource(createDuplicateResource(graph, fres));
           t1.resource(fres);
           r2.addRolePlayer(related2, fres);
           r1.addRolePlayer(related1, createDuplicateResource(graph, fres));
           return new String[] { indexOf(graph, sres), indexOf(graph, fres) };
        });
        ResourceDeduplicationTask task = new ResourceDeduplicationTask(); 
        task.start(checkpoint -> { throw new RuntimeException("No checkpoint expected."); }, 
                    Json.object(ResourceDeduplicationTask.KEYSPACE_CONFIG, keyspace()));        
        Assert.assertEquals(new Long(7), task.totalElimintated());
        transact(graph -> {
            for (String key : resourceKeys)
                Assert.assertTrue(checkUnique(graph, key));
            Resource<String> res = graph.admin().getConcept(Schema.ConceptProperty.INDEX, resourceKeys[0]);
            Assert.assertEquals(1, res.relations(related1).size());
            Assert.assertEquals(1, res.relations(related2).size());
            Resource<Float> fres = graph.admin().getConcept(Schema.ConceptProperty.INDEX, resourceKeys[1]);
            Assert.assertEquals(1, fres.relations(related1).size());
            Assert.assertEquals(1, fres.relations(related2).size());
        });
    }
    
    /**
     * Test the resources that are found to be unattached are deleted when the boolean flag for the job
     * is configured this way.
     */
    @Test
    @Ignore
    public void testDeleteUnattached() {
        final Entity [] entities = transact(graph -> {
            return new Entity[] { thing.addEntity(), thing.addEntity(), thing.addEntity() };            
        });
        
        String [] resourceKeys = transact(graph -> {
            
            Resource<String> sres = stringResource.putResource("string-1");          
            entities[0].resource(sres);
            entities[2].resource(createDuplicateResource(graph, sres));
            
            Resource<Float> fres = floatResource.putResource(1.69f);
            entities[1].resource(createDuplicateResource(graph, fres));
            entities[2].resource(createDuplicateResource(graph, fres));
            entities[0].resource(fres);
            
            return new String[] { indexOf(graph, sres), indexOf(graph, fres) };
        });                
        transact(graph -> {
            for (Entity e : entities)
                e.delete();
        });
        ResourceDeduplicationTask task = new ResourceDeduplicationTask(); 
        task.start(checkpoint -> { throw new RuntimeException("No checkpoint expected."); }, 
                    Json.object(ResourceDeduplicationTask.KEYSPACE_CONFIG, keyspace(),
                            ResourceDeduplicationTask.DELETE_UNATTACHED_CONFIG, true));        
        Assert.assertEquals(new Long(3), task.totalElimintated());        
        transact(graph -> {
            for (String key : resourceKeys)
                Assert.assertNull(graph.admin().getConcept(Schema.ConceptProperty.INDEX, key)); 
        });
    }
}
