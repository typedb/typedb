package io.mindmaps.core.implementation;

import io.mindmaps.core.exceptions.ConceptIdNotUniqueException;
import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class MindmapsTransactionLowLevelTest {

    private MindmapsTransactionImpl mindmapsGraph;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraphAccessManager(){
        mindmapsGraph = (MindmapsTransactionImpl) MindmapsTestGraphFactory.newEmptyGraph().newTransaction();
        mindmapsGraph.initialiseMetaConcepts();
    }
    @After
    public void destroyGraphAccessManager()  throws Exception{
        mindmapsGraph.close();
    }

    @Test
    public void testGetGraph(){
        assertThat(mindmapsGraph.getTinkerPopGraph(), instanceOf(Graph.class));
    }

    @Test
    public void testPutConcept() throws Exception {
        int numVerticies = 14;
        for(int i = 0; i < numVerticies; i ++)
            mindmapsGraph.putEntityType("c" + i);
        assertEquals(22, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
    }

    //----------------------------------------------Concept Functionality-----------------------------------------------

    @Test
    public void testPutConceptWithSubjectID() {
        Concept concept = mindmapsGraph.putEntityType("http://mindmaps.io").setSubject("http://mindmaps.io");
        assertEquals("http://mindmaps.io", concept.getSubject());
    }

    @Test
    public void testPutConceptWithMultiWithOverlap() {
        Concept valueBased = mindmapsGraph.putEntityType("valueBased");
        valueBased.setId("valueBased");

        Concept subjectIdBased = mindmapsGraph.putEntityType("subjectIdBased");
        subjectIdBased.setSubject("www.mind.io");

        Concept concept = mindmapsGraph.putEntityType("valueBased");

        expectedException.expect(ConceptIdNotUniqueException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.ID_NOT_UNIQUE.getMessage(concept.toString(), DataType.ConceptPropertyUnique.SUBJECT_IDENTIFIER.name(), "www.mind.io"))
        ));

        concept.setSubject("www.mind.io");
    }

    @Test
    public void testPutConceptWithMultiWithExistingNode() {
        Concept valueBased = mindmapsGraph.putEntityType("valueBased");
        valueBased.setId("valueBased");

        Concept nextConcept = mindmapsGraph.putEntityType("valueBased").setSubject("www.mind.io");

        assertEquals(valueBased, nextConcept);
    }

    @Test
    public void testSetTinkerPopGrap(){
        Graph graph1 = mindmapsGraph.getTinkerPopGraph();
        mindmapsGraph.setTinkerPopGraph(TinkerGraph.open());
        Graph graph2 = mindmapsGraph.getTinkerPopGraph();
        assertNotEquals(graph1, graph2);
    }

    @Test
    public void testMergeCastings(){
        RoleType roleType1 = mindmapsGraph.putRoleType("role 1");
        RoleType roleType2 = mindmapsGraph.putRoleType("role 2");
        RelationType relationType = mindmapsGraph.putRelationType("rel type").hasRole(roleType1).hasRole(roleType2);
        EntityType thing = mindmapsGraph.putEntityType("thing").playsRole(roleType1).playsRole(roleType2);
        Instance instance1 = mindmapsGraph.putEntity("1", thing);
        Instance instance2 = mindmapsGraph.putEntity("2", thing);
        Instance instance3 = mindmapsGraph.putEntity("3", thing);

        RelationImpl relation1 = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType).putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);
        RelationImpl relation2 = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType).putRolePlayer(roleType1, instance2).putRolePlayer(roleType2, instance3);

        Set<CastingImpl> castings = relation1.getMappingCasting();
        castings.addAll(relation2.getMappingCasting());
        Set<Concept> concepts = new HashSet<>();
        castings.forEach(concepts::add);

        assertEquals(4, mindmapsGraph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.CASTING.name()).toList().size());
        mindmapsGraph.mergeCastings(concepts);
        assertEquals(1, mindmapsGraph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.CASTING.name()).toList().size());
    }

    @Test
    public void testClearGraph(){
        mindmapsGraph.putEntityType("Hello");
        assertEquals(9, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
        mindmapsGraph.clearGraph();
        assertEquals(8, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
    }

    @Test
    public void getQuickTypeIdTest(){
        EntityType thing = mindmapsGraph.putEntityType("thing");
        Instance world = mindmapsGraph.putEntity("World", thing);
        assertEquals("thing", mindmapsGraph.getQuickTypeId(world));
    }

    @Test(expected=RuntimeException.class)
    public void testTooManyNodesForId() {
        Graph graph = mindmapsGraph.getTinkerPopGraph();
        Vertex v1 = graph.addVertex();
        v1.property(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), "value");
        Vertex v2 = graph.addVertex();
        v2.property(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), "value");
        mindmapsGraph.putEntityType("value");
    }

    @Test
    public void testGetConceptByBaseIdentifier() throws Exception {
        assertNull(mindmapsGraph.getConceptByBaseIdentifier(1000));

        ConceptImpl c1 = (ConceptImpl) mindmapsGraph.putEntityType("c1");
        ConceptImpl c2 = mindmapsGraph.getConceptByBaseIdentifier(c1.getBaseIdentifier());
        assertEquals(c1, c2);
    }

    @Test
    public void testGetConcept() throws Exception {
        Concept c1 = mindmapsGraph.putEntityType("VALUE");
        Concept c2 = mindmapsGraph.getConcept("VALUE");
        assertEquals(c1, c2);
    }

    @Test
    public void testGetConceptBySubjectIdentifier() throws Exception {
        Concept c1 = mindmapsGraph.putEntityType("c1").setSubject("www.mindmaps.io");
        Concept c2 = mindmapsGraph.getConceptBySubject("www.mindmaps.io");
        assertEquals(c1, c2);
    }

    //-----------------------------------------------Casting Functionality----------------------------------------------
    @Test
    public void testAddCastingLong() {
        //Build It
        RelationType relationType = mindmapsGraph.putRelationType("reltype");
        RoleTypeImpl role = (RoleTypeImpl) mindmapsGraph.putRoleType("Role");
        EntityType thing = mindmapsGraph.putEntityType("thing");
        InstanceImpl rolePlayer = (InstanceImpl) mindmapsGraph.putEntity("rolePlayer", thing);
        RelationImpl relation = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);
        CastingImpl casting = mindmapsGraph.putCasting(role, rolePlayer, relation);

        //Check it
        Vertex roleVertex = mindmapsGraph.getTinkerPopGraph().traversal().V(role.getBaseIdentifier()).next();
        Vertex rolePlayerVertex = mindmapsGraph.getTinkerPopGraph().traversal().V(rolePlayer.getBaseIdentifier()).next();
        Vertex assertionVertex = mindmapsGraph.getTinkerPopGraph().traversal().V(relation.getBaseIdentifier()).next();

        org.apache.tinkerpop.gremlin.structure.Edge casting_role = roleVertex.edges(Direction.IN).next();
        org.apache.tinkerpop.gremlin.structure.Edge casting_rolePlayer = rolePlayerVertex.edges(Direction.IN).next();

        assertEquals(casting.getBaseIdentifier(), casting_role.outVertex().id());
        assertEquals(casting.getBaseIdentifier(), casting_rolePlayer.outVertex().id());

        assertEquals(DataType.BaseType.ROLE_TYPE.name(), roleVertex.label());
        assertEquals(DataType.BaseType.RELATION.name(), assertionVertex.label());
    }

    @Test
    public void testAddCastingLongDuplicate(){
        RelationType relationType = mindmapsGraph.putRelationType("reltype");
        RoleTypeImpl role = (RoleTypeImpl) mindmapsGraph.putRoleType("Role");
        EntityType thing = mindmapsGraph.putEntityType("thing");
        InstanceImpl rolePlayer = (InstanceImpl) mindmapsGraph.putEntity("rolePlayer", thing);
        RelationImpl relation = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);
        CastingImpl casting1 = mindmapsGraph.putCasting(role, rolePlayer, relation);
        CastingImpl casting2 = mindmapsGraph.putCasting(role, rolePlayer, relation);
        assertEquals(casting1, casting2);
    }

    public void makeArtificialCasting(RoleTypeImpl role, InstanceImpl rolePlayer, RelationImpl relation) {
        String id = "FakeCasting " + UUID.randomUUID();
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(DataType.BaseType.CASTING.name());
        vertex.property(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), id);
        vertex.property(DataType.ConceptPropertyUnique.INDEX.name(), CastingImpl.generateNewHash(role, rolePlayer));

        CastingImpl casting = (CastingImpl) mindmapsGraph.getConcept(id);
        EdgeImpl edge = casting.addEdge(role, DataType.EdgeLabel.ISA); // Casting to Role
        edge.setEdgePropertyRoleType(role.getId());
        edge = casting.addEdge(rolePlayer, DataType.EdgeLabel.ROLE_PLAYER);// Casting to Roleplayer
        edge.setEdgePropertyRoleType(role.getId());
        relation.addEdge(casting, DataType.EdgeLabel.CASTING);// Assertion to Casting
    }

    @Test
    public void testAddCastingLongManyCastingFound() {
        //Artificially Make First Casting
        RelationType relationType = mindmapsGraph.putRelationType("RelationType");
        RoleTypeImpl role = (RoleTypeImpl) mindmapsGraph.putRoleType("role");
        EntityType thing = mindmapsGraph.putEntityType("thing");
        InstanceImpl rolePlayer = (InstanceImpl) mindmapsGraph.putEntity("rolePlayer", thing);
        RelationImpl relation = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);

        //First Casting
        makeArtificialCasting(role, rolePlayer, relation);

        //Second Casting Between same entities
        makeArtificialCasting(role, rolePlayer, relation);

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString("More than one casting found")
        ));

        mindmapsGraph.putCasting(role, rolePlayer, relation);
    }

    @Test
    public void testExpandingCastingWithRolePlayer() {
        RelationType relationType = mindmapsGraph.putRelationType("RelationType");
        EntityType type = mindmapsGraph.putEntityType("Parent");
        RoleTypeImpl role1 = (RoleTypeImpl) mindmapsGraph.putRoleType("Role1");
        RoleTypeImpl role2 = (RoleTypeImpl) mindmapsGraph.putRoleType("Role2");

        InstanceImpl<?, ?, ?> rolePlayer1 = (InstanceImpl) mindmapsGraph.putEntity("rolePlayer1", type);
        InstanceImpl<?, ?, ?> rolePlayer2 = (InstanceImpl) mindmapsGraph.putEntity("rolePlayer2", type);

        RelationImpl assertion = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType).
                putRolePlayer(role1, rolePlayer1).putRolePlayer(role2, null);
        CastingImpl casting1 = mindmapsGraph.putCasting(role1, rolePlayer1, assertion);
        CastingImpl casting2 = mindmapsGraph.putCasting(role2, rolePlayer2, assertion);

        assertTrue(assertion.getMappingCasting().contains(casting1));
        assertTrue(assertion.getMappingCasting().contains(casting2));
        assertNotEquals(casting1, casting2);

        Concept rolePlayer2Copy = rolePlayer1.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT).iterator().next();
        Concept rolePlayer1Copy = rolePlayer2.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT).iterator().next();

        assertEquals(rolePlayer1, rolePlayer1Copy);
        assertEquals(rolePlayer2, rolePlayer2Copy);
    }

    public void assertCorrectConceptSet(Collection conceptSet, Concept c1, Concept c2, Concept c3){
        assertEquals(2, conceptSet.size());
        assertTrue(conceptSet.contains(c1));
        assertFalse(conceptSet.contains(c2));
        assertTrue(conceptSet.contains(c3));
    }

    @Test
    public void testGetConceptsByValue(){
        assertEquals(0, mindmapsGraph.getConceptsByValue("Bob").size());
        EntityType type = mindmapsGraph.putEntityType("Parent");
        Entity c1= mindmapsGraph.putEntity("c1", type);
        c1.setValue("Bob");

        Type c2= mindmapsGraph.putEntityType("c2");
        c2.setValue("Bob");

        RoleType c3= mindmapsGraph.putRoleType("c3");
        c3.setValue("Bob");

        assertEquals(3, mindmapsGraph.getConceptsByValue("Bob").size());
        assertTrue(mindmapsGraph.getConceptsByValue("Bob").contains(c1));
        assertTrue(mindmapsGraph.getConceptsByValue("Bob").contains(c2));
        assertTrue(mindmapsGraph.getConceptsByValue("Bob").contains(c3));
    }

    @Test
    public void testGetConceptInstancesByValue(){
        EntityType type = mindmapsGraph.putEntityType("Parent");
        assertEquals(0, mindmapsGraph.getEntitiesByValue("Bob").size());
        Entity c1 = mindmapsGraph.putEntity("c1", type);
        c1.setValue("Bob");
        EntityType c2 = mindmapsGraph.putEntityType("c2");
        c2.setValue("Bob");
        Entity c3 = mindmapsGraph.putEntity("c3", type);
        c3.setValue("Bob");
        assertCorrectConceptSet(mindmapsGraph.getEntitiesByValue("Bob"), c1, c2, c3);
    }

    @Test
    public void testGetConceptTypeByValue(){
        assertEquals(0, mindmapsGraph.getEntityTypesByValue("Bob").size());
        EntityType c1 = mindmapsGraph.putEntityType("c1");
        c1.setValue("Bob");
        Entity c2 = mindmapsGraph.putEntity("c2", c1);
        c2.setValue("Bob");
        Type c3 = mindmapsGraph.putEntityType("c3");
        c3.setValue("Bob");
        assertCorrectConceptSet(mindmapsGraph.getEntityTypesByValue("Bob"), c1, c2, c3);
    }

    @Test
    public void testGetRelationTypeByValue(){
        assertEquals(0, mindmapsGraph.getRelationTypesByValue("Bob").size());
        RelationType c1 = mindmapsGraph.putRelationType("c1");
        c1.setValue("Bob");
        EntityType c2 = mindmapsGraph.putEntityType("c2");
        c2.setValue("Bob");
        RelationType c3 = mindmapsGraph.putRelationType("c3");
        c3.setValue("Bob");
        assertCorrectConceptSet(mindmapsGraph.getRelationTypesByValue("Bob"), c1, c2, c3);
    }

    @Test
    public void testGetRoleTypeByValue(){
        assertEquals(0, mindmapsGraph.getRoleTypesByValue("Bob").size());
        RoleType c1 = mindmapsGraph.putRoleType("c1");
        c1.setValue("Bob");
        EntityType c2 = mindmapsGraph.putEntityType("c2");
        c2.setValue("Bob");
        RoleType c3 = mindmapsGraph.putRoleType("c3");
        c3.setValue("Bob");
        assertCorrectConceptSet(mindmapsGraph.getRoleTypesByValue("Bob"), c1, c2, c3);
    }

    @Test
    public void testGetConceptInstance(){
        assertNull(mindmapsGraph.getEntityBySubject("Bob"));
        assertNull(mindmapsGraph.getEntity("Bob"));
        EntityType type = mindmapsGraph.putEntityType("Parent");
        Instance c1 = mindmapsGraph.putEntity("Bob1", type).setSubject("Bob");
        Instance c2 = mindmapsGraph.putEntity("Bob", type);
        assertEquals(c1, mindmapsGraph.getEntityBySubject("Bob"));
        assertEquals(c2, mindmapsGraph.getEntity("Bob"));
    }

    @Test
    public void testGetRelation(){
        RelationType relationType = mindmapsGraph.putRelationType("Hello");
        Relation c1 = mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);
        assertEquals(c1, mindmapsGraph.getRelation(c1.getId()));
        assertNull(mindmapsGraph.getResourceType("BOB"));
    }

    @Test
    public void testGetConceptType(){
        assertNull(mindmapsGraph.getEntityTypeBySubject("Bob"));
        assertNull(mindmapsGraph.getEntityType("Bob"));
        Type c1 = mindmapsGraph.putEntityType("Bob1").setSubject("Bob");
        Type c2 = mindmapsGraph.putEntityType("Bob");
        assertEquals(c1, mindmapsGraph.getEntityTypeBySubject("Bob"));
        assertEquals(c2, mindmapsGraph.getEntityType("Bob"));
    }

    @Test
    public void testGetRelationType(){
        assertNull(mindmapsGraph.getRelationTypeBySubject("Bob"));
        assertNull(mindmapsGraph.getRelationType("Bob"));
        RelationType c1 = mindmapsGraph.putRelationType("Bob1").setSubject("Bob");
        RelationType c2 = mindmapsGraph.putRelationType("Bob");
        assertEquals(c1, mindmapsGraph.getRelationTypeBySubject("Bob"));
        assertEquals(c2, mindmapsGraph.getRelationType("Bob"));
    }

    @Test
    public void testGetRoleType(){
        assertNull(mindmapsGraph.getRoleTypeBySubject("Bob"));
        assertNull(mindmapsGraph.getRoleType("Bob"));
        RoleType c1 = mindmapsGraph.putRoleType("Bob1").setSubject("Bob");
        RoleType c2 = mindmapsGraph.putRoleType("Bob");
        assertEquals(c1, mindmapsGraph.getRoleTypeBySubject("Bob"));
        assertEquals(c2, mindmapsGraph.getRoleType("Bob"));
    }

    @Test
    public void testGetResourceType(){
        assertNull(mindmapsGraph.getResourceTypeBySubject("Bob"));
        assertNull(mindmapsGraph.getResourceType("Bob"));
        ResourceType c1 = mindmapsGraph.putResourceType("Bob1", Data.STRING).setSubject("Bob").setValue("1");
        ResourceType c2 = mindmapsGraph.putResourceType("Bob", Data.STRING).setValue("1");
        assertEquals(c1, mindmapsGraph.getResourceTypeBySubject("Bob"));
        assertEquals(c2, mindmapsGraph.getResourceType("Bob"));
        assertEquals(2, mindmapsGraph.getResourceTypesByValue("1").size());
    }

    @Test
    public void testGetRuleType(){
        assertNull(mindmapsGraph.getRuleTypeBySubject("Bob"));
        assertNull(mindmapsGraph.getRuleType("Bob"));
        RuleType c1 = mindmapsGraph.putRuleType("Bob1").setSubject("Bob").setValue("1");
        RuleType c2 = mindmapsGraph.putRuleType("Bob").setValue("1");
        assertEquals(c1, mindmapsGraph.getRuleTypeBySubject("Bob"));
        assertEquals(c2, mindmapsGraph.getRuleType("Bob"));
        assertEquals(2, mindmapsGraph.getRuleTypesByValue("1").size());
    }

    @Test
    public void testGetResource(){
        assertNull(mindmapsGraph.getResourceBySubject("Bob"));
        assertNull(mindmapsGraph.getResource("Bob"));
        ResourceType type = mindmapsGraph.putResourceType("Type", Data.STRING);
        Resource c1 = mindmapsGraph.putResource("Bob1", type).setSubject("Bob").setValue("1");
        Resource c2 = mindmapsGraph.putResource("Bob", type).setValue("1");
        assertEquals(c1, mindmapsGraph.getResourceBySubject("Bob"));
        assertEquals(c2, mindmapsGraph.getResource("Bob"));
        assertEquals(2, mindmapsGraph.getResourcesByValue("1").size());
    }

    @Test
    public void testGetRule(){
        assertNull(mindmapsGraph.getRuleBySubject("Bob"));
        assertNull(mindmapsGraph.getRule("Bob"));
        RuleType type = mindmapsGraph.putRuleType("Type");
        RuleImpl c1 = (RuleImpl) mindmapsGraph.putRule("Bob1", type).setSubject("Bob").setValue("1");
        RuleImpl c2 = (RuleImpl) mindmapsGraph.putRule("Bob", type).setValue("1");
        assertEquals(c1, mindmapsGraph.getRuleBySubject("Bob"));
        assertEquals(c2, mindmapsGraph.getRule("Bob"));
        assertEquals(2, mindmapsGraph.getRulesByValue("1").size());
    }

    @Test
    public void getSuperConceptType(){
        assertEquals(mindmapsGraph.getMetaType().getId(), DataType.ConceptMeta.TYPE.getId());
    }

    @Test
    public void getSuperRelationType(){
        assertEquals(mindmapsGraph.getMetaRelationType().getId(), DataType.ConceptMeta.RELATION_TYPE.getId());
    }

    @Test
    public void getSuperRoleType(){
        assertEquals(mindmapsGraph.getMetaRoleType().getId(), DataType.ConceptMeta.ROLE_TYPE.getId());
    }

    @Test
    public void getSuperResourceType(){
        assertEquals(mindmapsGraph.getMetaResourceType().getId(), DataType.ConceptMeta.RESOURCE_TYPE.getId());
    }

    @Test
    public void testGetMetaRuleInference() {
        assertEquals(mindmapsGraph.getMetaRuleInference().getId(), DataType.ConceptMeta.INFERENCE_RULE.getId());
    }

    @Test
    public void testGetMetaRuleConstraint() {
        assertEquals(mindmapsGraph.getMetaRuleConstraint().getId(), DataType.ConceptMeta.CONSTRAINT_RULE.getId());
    }

    @Test
    public void testMetaOntologyInitialisation(){
        Type type = mindmapsGraph.getMetaType();
        Type relationType = mindmapsGraph.getMetaRelationType();
        Type roleType = mindmapsGraph.getMetaRoleType();
        Type resourceType = mindmapsGraph.getMetaResourceType();

        assertNotNull(type);
        assertNotNull(relationType);
        assertNotNull(roleType);
        assertNotNull(resourceType);

        assertEquals(type, relationType.superType());
        assertEquals(type, roleType.superType());
        assertEquals(type, resourceType.superType());
    }

    @Test
    public void testBatchLoadingMode(){
        mindmapsGraph.enableBatchLoading();

        EntityType type = mindmapsGraph.putEntityType("Concept Type");
        RoleTypeImpl feature = (RoleTypeImpl) mindmapsGraph.putRoleType("Feature");
        RoleTypeImpl actor = (RoleTypeImpl) mindmapsGraph.putRoleType("Actor");
        InstanceImpl pacino = (InstanceImpl) mindmapsGraph.putEntity("Pacino", type);
        InstanceImpl godfather = (InstanceImpl) mindmapsGraph.putEntity("Godfather", type);
        RelationTypeImpl cast = (RelationTypeImpl) mindmapsGraph.putRelationType("Cast");

        cast.hasRole(feature);
        cast.hasRole(actor);

        mindmapsGraph.putRelation(UUID.randomUUID().toString(), cast).
                putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);

        assertTrue(mindmapsGraph.getTinkerPopGraph().traversal().E().hasLabel(DataType.EdgeLabel.SHORTCUT.getLabel()).hasNext());

        mindmapsGraph.disableBatchLoading();

        InstanceImpl x = (InstanceImpl) mindmapsGraph.putEntity("x", type);
        InstanceImpl y = (InstanceImpl) mindmapsGraph.putEntity("x", type);

        mindmapsGraph.putRelation(UUID.randomUUID().toString(), cast).
                putRolePlayer(feature, x).putRolePlayer(actor, y);

        assertTrue(mindmapsGraph.getTinkerPopGraph().traversal().E().hasLabel(DataType.EdgeLabel.SHORTCUT.getLabel()).hasNext());
    }

    @Test
    public void checkTypeCreation(){
        Type testType = mindmapsGraph.putEntityType("Test Concept Type");
        ResourceType testResourceType = mindmapsGraph.putResourceType("Test Resource Type", Data.STRING);
        RoleType testRoleType = mindmapsGraph.putRoleType("Test Role Type");
        RelationType testRelationType = mindmapsGraph.putRelationType("Test Relation Type");

        assertEquals(DataType.ConceptMeta.TYPE.getId(), testType.type().type().getId());
        assertEquals(DataType.ConceptMeta.ENTITY_TYPE.getId(), testType.type().getId());
        assertEquals(DataType.ConceptMeta.RESOURCE_TYPE.getId(), testResourceType.type().getId());
        assertEquals(DataType.ConceptMeta.ROLE_TYPE.getId(), testRoleType.type().getId());
        assertEquals(DataType.ConceptMeta.RELATION_TYPE.getId(), testRelationType.type().getId());

    }

    @Test
    public void checkPostprocessingHelpers(){
        EntityType movie = mindmapsGraph.putEntityType("movie");
        EntityType actor = mindmapsGraph.putEntityType("actor");
        RoleType movieRole = mindmapsGraph.putRoleType("movie role");
        RoleType actorRole = mindmapsGraph.putRoleType("actorRole");
        InstanceImpl godfather = (InstanceImpl) mindmapsGraph.putEntity("Godfather", movie);
        InstanceImpl pacino = (InstanceImpl) mindmapsGraph.putEntity("Godfather", actor);
        RelationTypeImpl relationType = (RelationTypeImpl) mindmapsGraph.putRelationType("Relation Type").hasRole(movieRole).hasRole(actorRole);
        RelationImpl relation = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType).putRolePlayer(movieRole, godfather).putRolePlayer(actorRole, pacino);

        assertEquals(13, mindmapsGraph.getModifiedConcepts().size());
        assertEquals(2, mindmapsGraph.getModifiedCastingIds().size());
        assertEquals(1, mindmapsGraph.getModifiedRelationIds().size());

        Set<CastingImpl> castings = relation.getMappingCasting();
        Stream<Long> castingIds = castings.stream().map(CastingImpl::getBaseIdentifier);
        List<Long> sortedIds = castingIds.sorted().collect(Collectors.toList());


        assertEquals(relation.getBaseIdentifier() + "_" + sortedIds.get(0) + "." + sortedIds.get(1) + ".", mindmapsGraph.getUniqueRelationId(relation));
    }

    @Test
    public void testGetType(){
        EntityType a = mindmapsGraph.putEntityType("a").setValue("1");
        RoleType b = mindmapsGraph.putRoleType("b").setValue("1").setSubject("subject");
        RelationType c = mindmapsGraph.putRelationType("c").setValue("1");

        assertEquals(a, mindmapsGraph.getType("a"));
        Collection<Type> set = mindmapsGraph.getTypesByValue("1");
        assertEquals(3, mindmapsGraph.getTypesByValue("1").size());
        assertTrue(set.contains(a));
        assertTrue(set.contains(b));
        assertTrue(set.contains(c));

        assertEquals(b, mindmapsGraph.getTypeBySubject("subject"));
    }

    @Test
    public void testInstance(){
        EntityType a = mindmapsGraph.putEntityType("a");
        RelationType b = mindmapsGraph.putRelationType("b");
        ResourceType<String> c = mindmapsGraph.putResourceType("c", Data.STRING);

        Entity instanceA = mindmapsGraph.putEntity("instanceA", a).setValue("1");
        Relation instanceB = mindmapsGraph.putRelation(UUID.randomUUID().toString(), b).setValue("1").setSubject("subject");
        Resource<String> instanceC = mindmapsGraph.putResource("instanceC", c).setValue("1");

        assertEquals(instanceA, mindmapsGraph.getInstance("instanceA"));
        Collection<Instance> set = mindmapsGraph.getInstancesByValue("1");
        assertEquals(3, set.size());
        assertTrue(set.contains(instanceA));
        assertTrue(set.contains(instanceB));
        assertTrue(set.contains(instanceC));

        assertEquals(instanceB, mindmapsGraph.getInstanceBySubject("subject"));
    }

    @Test
    public void testComplexDelete() throws MindmapsValidationException {
        RoleType roleType1 = mindmapsGraph.putRoleType("roleType 1");
        RoleType roleType2 = mindmapsGraph.putRoleType("roleType 2");
        RoleType roleType3 = mindmapsGraph.putRoleType("roleType 3");
        RoleType roleType4 = mindmapsGraph.putRoleType("roleType 4");
        EntityType entityType = mindmapsGraph.putEntityType("entity type").
                playsRole(roleType1).playsRole(roleType2).
                playsRole(roleType3).playsRole(roleType4);
        RelationType relationType1 = mindmapsGraph.putRelationType("relation type 1").hasRole(roleType1).hasRole(roleType2);
        RelationType relationType2 = mindmapsGraph.putRelationType("relation type 2").hasRole(roleType3).hasRole(roleType4);

        Entity entity1 = mindmapsGraph.putEntity("1", entityType);
        Entity entity2 = mindmapsGraph.putEntity("2", entityType);
        Entity entity3 = mindmapsGraph.putEntity("3", entityType);
        Entity entity4 = mindmapsGraph.putEntity("4", entityType);
        Entity entity5 = mindmapsGraph.putEntity("5", entityType);

        mindmapsGraph.addRelation(relationType1).putRolePlayer(roleType1, entity1).putRolePlayer(roleType2, entity2);
        mindmapsGraph.addRelation(relationType1).putRolePlayer(roleType1, entity1).putRolePlayer(roleType2, entity3);
        mindmapsGraph.addRelation(relationType2).putRolePlayer(roleType3, entity1).putRolePlayer(roleType4, entity4);
        mindmapsGraph.addRelation(relationType2).putRolePlayer(roleType3, entity1).putRolePlayer(roleType4, entity5);

        mindmapsGraph.commit();

        entity1.delete();

        mindmapsGraph.commit();

        assertNull(mindmapsGraph.getConcept("1"));
    }

    @Test
    public void testPutRelationTest(){
        RoleType roleType1 = mindmapsGraph.putRoleType("roleType 1");
        RoleType roleType2 = mindmapsGraph.putRoleType("roleType 2");
        EntityType entityType = mindmapsGraph.putEntityType("entity type").
                playsRole(roleType1).playsRole(roleType2);
        RelationType relationType1 = mindmapsGraph.putRelationType("relation type 1").hasRole(roleType1).hasRole(roleType2);

        Entity entity1 = mindmapsGraph.putEntity("1", entityType);
        Entity entity2 = mindmapsGraph.putEntity("2", entityType);

        Map<RoleType, Instance> map = new HashMap<>();
        map.put(roleType1, entity1);
        map.put(roleType2, entity2);

        Relation rel1 = mindmapsGraph.putRelation(relationType1, map);
        Relation rel2 = mindmapsGraph.putRelation(relationType1, map);
    }

}