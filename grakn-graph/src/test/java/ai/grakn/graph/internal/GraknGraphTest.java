package ai.grakn.graph.internal;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GraknGraphTest extends GraphTestBase {
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
        final boolean[] errorNotThrown = {false};

        Future future = pool.submit(() -> {
            try{
                graph.putEntityType("A Thing");
            } catch (GraphRuntimeException e){
                if(e.getMessage().equals(ErrorMessage.GRAPH_CLOSED.getMessage(graph.getKeyspace()))){
                    errorThrown[0] = true;
                }
            }

            graph.open();
            graph.putEntityType("A Thing");
            errorNotThrown[0] = true;
        });

        future.get();

        assertTrue("Error not thrown when graph is closed in another thread", errorThrown[0]);
        assertTrue("Error thrown even after opening graph", errorNotThrown[0]);
    }

    @Test
    public void testCloseAndReOpenGraph(){
        GraknGraph graph = Grakn.factory(Grakn.IN_MEMORY, "testing").getGraph();
        graph.close();

        boolean errorThrown = false;
        try{
            graph.putEntityType("A Thing");
        } catch (GraphRuntimeException e){
            if(e.getMessage().equals(ErrorMessage.CLOSED_USER.getMessage())){
                errorThrown = true;
            }
        }
        assertTrue("Graph not correctly closed", errorThrown);

        graph.open();

        graph.putEntityType("A Thing");
    }
}
