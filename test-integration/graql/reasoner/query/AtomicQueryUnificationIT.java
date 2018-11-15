


package grakn.core.graql.reasoner.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Relationship;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.Graql;
import grakn.core.graql.Query;
import grakn.core.graql.Var;
import grakn.core.graql.admin.Conjunction;
import grakn.core.graql.admin.MultiUnifier;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.pattern.Patterns;
import grakn.core.graql.internal.query.answer.ConceptMapImpl;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryEquivalence;
import grakn.core.graql.internal.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.internal.reasoner.unifier.UnifierType;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.session.TransactionImpl;
import grakn.core.server.session.SessionImpl;
import grakn.core.rule.ConcurrentGraknServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.graql.Graql.var;
import static grakn.core.graql.reasoner.query.TestQueryPattern.subList;
import static grakn.core.graql.reasoner.query.TestQueryPattern.subListExcluding;
import static grakn.core.graql.reasoner.query.TestQueryPattern.subListExcludingElements;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.collections.CollectionUtils.isEqualCollection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class AtomicQueryUnificationIT {

    @ClassRule
    public static final ConcurrentGraknServer server = new ConcurrentGraknServer();

    private static SessionImpl genericSchemaSession;
    private static SessionImpl unificationWithTypesSession;

    private static void loadFromFile(String fileName, Session session){
        try {
            InputStream inputStream = AtomicQueryUnificationIT.class.getClassLoader().getResourceAsStream("test-integration/graql/reasoner/resources/"+fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            tx.graql().parser().parseList(s).forEach(Query::execute);
            tx.commit();
        } catch (Exception e){
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    private static Entity entity;
    private static Entity anotherEntity;
    private static Entity anotherBaseEntity;
    private static Entity subEntity;
    private static Relationship relation;
    private static Relationship anotherRelation;
    private static Attribute<Object> resource;
    private static Attribute<Object> anotherResource;


    @BeforeClass
    public static void loadContext(){
        genericSchemaSession = server.sessionWithNewKeyspace();
        loadFromFile("genericSchema.gql", genericSchemaSession);
        unificationWithTypesSession = server.sessionWithNewKeyspace();
        loadFromFile("unificationWithTypesTest.gql", unificationWithTypesSession);
        try(Transaction tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            EntityType subRoleEntityType = tx.getEntityType("subRoleEntity");
            Iterator<Entity> entities = tx.getEntityType("baseRoleEntity").instances()
                    .filter(et -> !et.type().equals(subRoleEntityType) )
                    .collect(toSet()).iterator();
            entity = entities.next();
            anotherEntity = entities.next();
            anotherBaseEntity = tx.getEntityType("anotherBaseRoleEntity").instances().findFirst().orElse(null);
            subEntity = tx.getEntityType("subRoleEntity").instances().findFirst().orElse(null);
            Iterator<Relationship> relations = tx.getRelationshipType("baseRelation").subs().flatMap(RelationshipType::instances).iterator();
            relation = relations.next();
            anotherRelation = relations.next();
            Iterator<Attribute<Object>> resources = tx.getAttributeType("resource").instances().collect(toSet()).iterator();
            resource = resources.next();
            anotherResource = resources.next();
        }
    }

    @AfterClass
    public static void closeSession(){
        genericSchemaSession.close();
        unificationWithTypesSession.close();
    }

    @Test
    public void testUnification_RULE_BinaryRelationWithSubs(){
        try(TransactionImpl<?> tx =  unificationWithTypesSession.transaction(Transaction.Type.WRITE)) {
            Concept x1 = getConceptByResourceValue(tx, "x1");
            Concept x2 = getConceptByResourceValue(tx, "x2");

            ReasonerAtomicQuery xbaseQuery = ReasonerQueries.atomic(conjunction("{($x1, $x2) isa binary;}"), tx);
            ReasonerAtomicQuery ybaseQuery = ReasonerQueries.atomic(conjunction("{($y1, $y2) isa binary;}"), tx);

            ConceptMap xAnswer = new ConceptMapImpl(ImmutableMap.of(var("x1"), x1, var("x2"), x2));
            ConceptMap flippedXAnswer = new ConceptMapImpl(ImmutableMap.of(var("x1"), x2, var("x2"), x1));

            ConceptMap yAnswer = new ConceptMapImpl(ImmutableMap.of(var("y1"), x1, var("y2"), x2));
            ConceptMap flippedYAnswer = new ConceptMapImpl(ImmutableMap.of(var("y1"), x2, var("y2"), x1));

            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(xbaseQuery, xAnswer);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(xbaseQuery, flippedXAnswer);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("x1"), var("x2"),
                    var("x2"), var("x1")
            ));
            assertTrue(unifier.equals(correctUnifier));

            ReasonerAtomicQuery yChildQuery = ReasonerQueries.atomic(ybaseQuery, yAnswer);
            ReasonerAtomicQuery yChildQuery2 = ReasonerQueries.atomic(ybaseQuery, flippedYAnswer);

            MultiUnifier unifier2 = yChildQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier2 = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("y1"), var("x1"),
                    var("y2"), var("x2")
            ));
            assertTrue(unifier2.equals(correctUnifier2));

            MultiUnifier unifier3 = yChildQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier3 = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("y1"), var("x2"),
                    var("y2"), var("x1")
            ));
            assertTrue(unifier3.equals(correctUnifier3));
        }
    }

    @Test //only a single unifier exists
    public void testUnification_EXACT_BinaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        try(TransactionImpl<?> tx =  unificationWithTypesSession.transaction(Transaction.Type.WRITE)){
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{$x1 isa twoRoleEntity;($x1, $x2) isa binary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{$y1 isa twoRoleEntity;($y1, $y2) isa binary;}"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("y1"), var("x1"),
                    var("y2"), var("x2")
            ));
            assertEquals(unifier, correctUnifier);
        }
    }

    @Test //only a single unifier exists
    public void testUnification_EXACT_BinaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes(){
        try(TransactionImpl<?> tx =  unificationWithTypesSession.transaction(Transaction.Type.WRITE)){
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{$x1 isa twoRoleEntity;$x2 isa twoRoleEntity2;($x1, $x2) isa binary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{$y1 isa twoRoleEntity;$y2 isa twoRoleEntity2;($y1, $y2) isa binary;}"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("y1"), var("x1"),
                    var("y2"), var("x2")
            ));
            assertEquals(unifier, correctUnifier);
        }
    }

    @Test
    public void testUnification_EXACT_TernaryRelation_ParentRepeatsRoles(){
        try(TransactionImpl<?> tx =  unificationWithTypesSession.transaction(Transaction.Type.WRITE)){
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{(role1: $x, role1: $y, role2: $z) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role3: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $u, role2: $q) isa ternary;}"), tx);

            MultiUnifier emptyUnifier = childQuery.getMultiUnifier(parentQuery);
            MultiUnifier emptyUnifier2 = childQuery2.getMultiUnifier(parentQuery);
            MultiUnifier emptyUnifier3 = childQuery4.getMultiUnifier(parentQuery);

            assertEquals(emptyUnifier, MultiUnifierImpl.nonExistent());
            assertEquals(emptyUnifier2, MultiUnifierImpl.nonExistent());
            assertEquals(emptyUnifier3, MultiUnifierImpl.nonExistent());

            MultiUnifier unifier = childQuery3.getMultiUnifier(parentQuery);
            MultiUnifier correctUnifier = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("v"), var("y"),
                            var("q"), var("z")),
                    ImmutableMultimap.of(
                            var("u"), var("y"),
                            var("v"), var("x"),
                            var("q"), var("z"))
            );
            assertEquals(unifier, correctUnifier);
            assertEquals(2, unifier.size());
        }
    }

    @Test
    public void testUnification_EXACT_TernaryRelation_ParentRepeatsMetaRoles_ParentRepeatsRPs(){
        try(TransactionImpl<?> tx =  unificationWithTypesSession.transaction(Transaction.Type.WRITE)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{(role: $x, role: $x, role2: $y) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role3: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $u, role2: $q) isa ternary;}"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("q"), var("x"),
                            var("u"), var("x"),
                            var("v"), var("y"))
            );
            assertEquals(unifier, correctUnifier);

            MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier2 = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("q"), var("x"),
                            var("v"), var("y")),
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("v"), var("x"),
                            var("q"), var("y"))
            );
            assertEquals(unifier2, correctUnifier2);
            assertEquals(unifier2.size(), 2);

            MultiUnifier unifier3 = childQuery3.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier3 = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("v"), var("x"),
                            var("q"), var("y"))
            );
            assertEquals(unifier3, correctUnifier3);

            MultiUnifier unifier4 = childQuery4.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier4 = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("u"), var("x"),
                    var("q"), var("y")
            ));
            assertEquals(unifier4, correctUnifier4);
        }
    }

    @Test
    public void testUnification_EXACT_TernaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        try(TransactionImpl<?> tx =  unificationWithTypesSession.transaction(Transaction.Type.WRITE)){
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{$x1 isa threeRoleEntity;$x3 isa threeRoleEntity3;($x1, $x2, $x3) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{$y3 isa threeRoleEntity3;$y1 isa threeRoleEntity;($y2, $y3, $y1) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{$y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.EXACT);
            MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.EXACT);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("y1"), var("x1"),
                    var("y2"), var("x2"),
                    var("y3"), var("x3")
            ));
            assertEquals(unifier, correctUnifier);
            assertEquals(unifier2, MultiUnifierImpl.nonExistent());
        }
    }

    @Test
    public void testUnification_RULE_TernaryRelation_ParentRepeatsMetaRoles(){
        try(TransactionImpl<?> tx =  unificationWithTypesSession.transaction(Transaction.Type.WRITE)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{(role: $x, role: $y, role2: $z) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role3: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $u, role2: $q) isa ternary;}"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("v"), var("z"),
                            var("q"), var("y")),
                    ImmutableMultimap.of(
                            var("u"), var("y"),
                            var("v"), var("z"),
                            var("q"), var("x"))
            );
            assertEquals(unifier, correctUnifier);
            assertEquals(unifier.size(), 2);

            MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier2 = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("v"), var("y"),
                            var("q"), var("z")),
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("v"), var("z"),
                            var("q"), var("y")),
                    ImmutableMultimap.of(
                            var("u"), var("y"),
                            var("v"), var("z"),
                            var("q"), var("x")),
                    ImmutableMultimap.of(
                            var("u"), var("y"),
                            var("v"), var("x"),
                            var("q"), var("z"))
            );
            assertEquals(unifier2, correctUnifier2);
            assertEquals(unifier2.size(), 4);

            MultiUnifier unifier3 = childQuery3.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier3 = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("u"), var("x"),
                            var("v"), var("y"),
                            var("q"), var("z")),
                    ImmutableMultimap.of(
                            var("u"), var("y"),
                            var("v"), var("x"),
                            var("q"), var("z"))
            );
            assertEquals(unifier3, correctUnifier3);
            assertEquals(unifier3.size(), 2);

            MultiUnifier unifier4 = childQuery4.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier4 = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("u"), var("x"),
                    var("u"), var("y"),
                    var("q"), var("z")
            ));
            assertEquals(unifier4, correctUnifier4);
        }
    }

    @Test
    public void testUnification_RULE_TernaryRelation_ParentRepeatsRoles_ParentRepeatsRPs(){
        try(TransactionImpl<?> tx =  unificationWithTypesSession.transaction(Transaction.Type.WRITE)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{(role1: $x, role1: $x, role2: $y) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role3: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{(role1: $u, role2: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $v, role2: $q) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{(role1: $u, role1: $u, role2: $q) isa ternary;}"), tx);

            MultiUnifier emptyUnifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier emptyUnifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);

            assertTrue(emptyUnifier.isEmpty());
            assertTrue(emptyUnifier2.isEmpty());

            MultiUnifier unifier = childQuery3.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("u"), var("x"),
                    var("v"), var("x"),
                    var("q"), var("y")
            ));
            assertEquals(unifier, correctUnifier);

            MultiUnifier unifier2 = childQuery4.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier2 = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("u"), var("x"),
                    var("q"), var("y")
            ));
            assertEquals(unifier2, correctUnifier2);
        }
    }

    @Test
    public void testUnification_RULE_TernaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes(){
        try(TransactionImpl<?> tx =  unificationWithTypesSession.transaction(Transaction.Type.WRITE)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{$x1 isa threeRoleEntity;$x2 isa threeRoleEntity2; $x3 isa threeRoleEntity3;($x1, $x2, $x3) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{$y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;($y2, $y3, $y1) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{$y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    var("y1"), var("x1"),
                    var("y2"), var("x2"),
                    var("y3"), var("x3")
            ));
            assertEquals(unifier, correctUnifier);
            assertEquals(unifier2, correctUnifier);
        }
    }

    @Test // subSubThreeRoleEntity sub subThreeRoleEntity sub threeRoleEntity3
    public void testUnification_RULE_TernaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes_TypeHierarchyInvolved(){
        try(TransactionImpl<?> tx =  unificationWithTypesSession.transaction(Transaction.Type.WRITE)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{$x1 isa threeRoleEntity;$x2 isa subThreeRoleEntity; $x3 isa subSubThreeRoleEntity;($x1, $x2, $x3) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{$y1 isa threeRoleEntity;$y2 isa subThreeRoleEntity;$y3 isa subSubThreeRoleEntity;($y2, $y3, $y1) isa ternary;}"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{$y1 isa threeRoleEntity;$y2 isa subThreeRoleEntity;$y3 isa subSubThreeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary;}"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            var("y1"), var("x1"),
                            var("y2"), var("x2"),
                            var("y3"), var("x3")),
                    ImmutableMultimap.of(
                            var("y1"), var("x1"),
                            var("y2"), var("x3"),
                            var("y3"), var("x2")),
                    ImmutableMultimap.of(
                            var("y1"), var("x2"),
                            var("y2"), var("x1"),
                            var("y3"), var("x3")),
                    ImmutableMultimap.of(
                            var("y1"), var("x2"),
                            var("y2"), var("x3"),
                            var("y3"), var("x1")),
                    ImmutableMultimap.of(
                            var("y1"), var("x3"),
                            var("y2"), var("x1"),
                            var("y3"), var("x2")),
                    ImmutableMultimap.of(
                            var("y1"), var("x3"),
                            var("y2"), var("x2"),
                            var("y3"), var("x1"))
            );
            assertTrue(unifier.equals(correctUnifier));
            assertTrue(unifier2.equals(correctUnifier));
        }
    }


    @Test
    public void testUnification_RULE_ResourcesWithTypes(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            String parentQuery = "{$x has resource $r; $x isa baseRoleEntity;}";

            String childQuery = "{$r has resource $x; $r isa subRoleEntity;}";
            String childQuery2 = "{$x1 has resource $x; $x1 isa subSubRoleEntity;}";
            String baseQuery = "{$r has resource $x; $r isa entity;}";

            unificationWithResultChecks(parentQuery, childQuery, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentQuery, childQuery2, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentQuery, baseQuery, true, true, true, UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_RULE_BinaryRelationWithRoleAndTypeHierarchy_MetaTypeParent(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            String parentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa entity; $y isa entity;}";

            String specialisedRelation = "{(subRole1: $u, anotherSubRole2: $v); $u isa baseRoleEntity; $v isa baseRoleEntity;}";
            String specialisedRelation2 = "{(subRole1: $y, anotherSubRole2: $x); $y isa baseRoleEntity; $x isa baseRoleEntity;}";
            String specialisedRelation3 = "{(subRole1: $u, anotherSubRole2: $v); $u isa subRoleEntity; $v isa subRoleEntity;}";
            String specialisedRelation4 = "{(subRole1: $y, anotherSubRole2: $x); $y isa subRoleEntity; $x isa subRoleEntity;}";
            String specialisedRelation5 = "{(subSubRole1: $u, subSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
            String specialisedRelation6 = "{(subSubRole1: $y, subSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";

            unificationWithResultChecks(parentRelation, specialisedRelation, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation2, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation3, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation4, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation5, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation6, false, false, true, UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_RULE_BinaryRelationWithRoleAndTypeHierarchy_BaseRoleParent(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            String baseParentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa baseRoleEntity; $y isa baseRoleEntity;}";
            String parentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa subSubRoleEntity; $y isa subSubRoleEntity;}";

            String specialisedRelation = "{(subRole1: $u, anotherSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
            String specialisedRelation2 = "{(subRole1: $y, anotherSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";
            String specialisedRelation3 = "{(subSubRole1: $u, subSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
            String specialisedRelation4 = "{(subSubRole1: $y, subSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";

            unificationWithResultChecks(baseParentRelation, specialisedRelation, false, false, true, UnifierType.RULE,  tx);
            unificationWithResultChecks(baseParentRelation, specialisedRelation2, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(baseParentRelation, specialisedRelation3, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(baseParentRelation, specialisedRelation4, false, false, true, UnifierType.RULE, tx);

            unificationWithResultChecks(parentRelation, specialisedRelation, false, false, false, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation2, false, false, false, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation3, false, false, false, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation4, false, false, false, UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_RULE_BinaryRelationWithRoleAndTypeHierarchy_BaseRoleParent_middleTypes(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            String parentRelation = "{(baseRole1: $x, baseRole2: $y); $x isa subRoleEntity; $y isa subRoleEntity;}";

            String specialisedRelation = "{(subRole1: $u, anotherSubRole2: $v); $u isa subRoleEntity; $v isa subSubRoleEntity;}";
            String specialisedRelation2 = "{(subRole1: $y, anotherSubRole2: $x); $y isa subRoleEntity; $x isa subSubRoleEntity;}";
            String specialisedRelation3 = "{(subSubRole1: $u, subSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity;}";
            String specialisedRelation4 = "{(subSubRole1: $y, subSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity;}";

            unificationWithResultChecks(parentRelation, specialisedRelation, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation2, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation3, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation4, false, false, true, UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariants_EXACT(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            List<String> qs = grakn.core.graql.reasoner.query.TestQueryPattern.differentRelationVariants.patternList(entity, anotherBaseEntity, subEntity);
            qs.forEach(q -> exactUnification(q, qs, new ArrayList<>(), tx));
        }
    }

    @Test
    public void testUnification_differentRelationVariants_STRUCTURAL(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            List<String> qs = grakn.core.graql.reasoner.query.TestQueryPattern.differentRelationVariants.patternList(entity, anotherBaseEntity, subEntity);

            structuralUnification(qs.get(0), qs, new ArrayList<>(), tx);

            structuralUnification(qs.get(1), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(2), qs, subList(qs, Lists.newArrayList(3, 4)), tx);
            structuralUnification(qs.get(3), qs, subList(qs, Lists.newArrayList(2, 4)), tx);
            structuralUnification(qs.get(4), qs, subList(qs, Lists.newArrayList(2, 3)), tx);

            structuralUnification(qs.get(5), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(6), qs, subList(qs, Lists.newArrayList(7, 8)), tx);
            structuralUnification(qs.get(7), qs, subList(qs, Lists.newArrayList(6, 8)), tx);
            structuralUnification(qs.get(8), qs, subList(qs, Lists.newArrayList(6, 7)), tx);

            structuralUnification(qs.get(9), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(10), qs, subList(qs, Lists.newArrayList(11)), tx);
            structuralUnification(qs.get(11), qs, subList(qs, Lists.newArrayList(10)), tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariants_RULE(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            List<String> qs = grakn.core.graql.reasoner.query.TestQueryPattern.differentRelationVariants.patternList(entity, anotherBaseEntity, subEntity);

            ruleUnification(qs.get(0), qs, qs, tx);

            ruleUnification(qs.get(1), qs, subListExcluding(qs, Lists.newArrayList(3)), tx);
            ruleUnification(qs.get(2), qs, subListExcluding(qs, Lists.newArrayList(3, 4, 11)), tx);
            ruleUnification(qs.get(3), qs, subListExcluding(qs, Lists.newArrayList(1, 2, 4, 9, 10, 11)), tx);
            ruleUnification(qs.get(4), qs, subListExcluding(qs, Lists.newArrayList(2, 3, 10)), tx);

            ruleUnification(qs.get(5), qs, subListExcluding(qs, Lists.newArrayList(7, 9, 10, 11)), tx);
            ruleUnification(qs.get(6), qs, subListExcluding(qs, Lists.newArrayList(7, 8, 9, 10, 11)), tx);
            ruleUnification(qs.get(7), qs, subListExcluding(qs, Lists.newArrayList(5, 6, 8)), tx);
            ruleUnification(qs.get(8), qs, subListExcluding(qs, Lists.newArrayList(6, 7, 9, 10, 11)), tx);

            ruleUnification(qs.get(9), qs, subListExcluding(qs, Lists.newArrayList(3, 5, 6, 8)), tx);
            ruleUnification(qs.get(10), qs, subListExcluding(qs, Lists.newArrayList(3, 4, 5, 6, 8, 11)), tx);
            ruleUnification(qs.get(11), qs, subListExcluding(qs, Lists.newArrayList(2, 3, 5, 6, 8, 10)), tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariantsWithRelationVariable_EXACT(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            List<String> qs = TestQueryPattern.differentRelationVariantsWithRelationVariable.patternList(entity, anotherBaseEntity, subEntity, relation, anotherRelation);

            exactUnification(qs.get(0), qs, subList(qs, Lists.newArrayList(1)), tx);
            exactUnification(qs.get(1), qs, subList(qs, Lists.newArrayList(0)), tx);
            subListExcluding(qs, Lists.newArrayList(0, 1)).forEach(q -> exactUnification(q, qs, new ArrayList<>(), tx));
        }
    }

    @Test
    public void testUnification_differentRelationVariantsWithRelationVariable_STRUCTURAL(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            List<String> qs = TestQueryPattern.differentRelationVariantsWithRelationVariable.patternList(entity, anotherBaseEntity, subEntity, relation, anotherRelation);

            structuralUnification(qs.get(0), qs, subList(qs, Lists.newArrayList(1)), tx);
            structuralUnification(qs.get(1), qs, subList(qs, Lists.newArrayList(0)), tx);
            structuralUnification(qs.get(2), qs, new ArrayList<>(), tx);

            structuralUnification(qs.get(3), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(4), qs, subList(qs, Lists.newArrayList(5, 6)), tx);
            structuralUnification(qs.get(5), qs, subList(qs, Lists.newArrayList(4, 6)), tx);
            structuralUnification(qs.get(6), qs, subList(qs, Lists.newArrayList(4, 5)), tx);

            structuralUnification(qs.get(7), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(8), qs, subList(qs, Lists.newArrayList(9, 10)), tx);
            structuralUnification(qs.get(9), qs, subList(qs, Lists.newArrayList(8, 10)), tx);
            structuralUnification(qs.get(10), qs, subList(qs, Lists.newArrayList(8, 9)), tx);

            structuralUnification(qs.get(11), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(12), qs, subList(qs, Lists.newArrayList(13)), tx);
            structuralUnification(qs.get(13), qs, subList(qs, Lists.newArrayList(12)), tx);

            structuralUnification(qs.get(14), qs, subList(qs, Lists.newArrayList(15)), tx);
            structuralUnification(qs.get(15), qs, subList(qs, Lists.newArrayList(14)), tx);
            structuralUnification(qs.get(16), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(17), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(18), qs, new ArrayList<>(), tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariantsWithRelationVariable_RULE(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            List<String> qs = TestQueryPattern.differentRelationVariantsWithRelationVariable.patternList(entity, anotherBaseEntity, subEntity, relation, anotherRelation);

            ruleUnification(qs.get(0), qs, subList(qs, Lists.newArrayList(1)), tx);
            ruleUnification(qs.get(1), qs, subList(qs, Lists.newArrayList(0)), tx);
            ruleUnification(qs.get(2), qs, qs, tx);

            ruleUnification(qs.get(3), qs, subListExcluding(qs, Lists.newArrayList(5, 16)), tx);
            ruleUnification(qs.get(4), qs, subListExcluding(qs, Lists.newArrayList(5, 6, 13, 16)), tx);
            ruleUnification(qs.get(5), qs, subList(qs, Lists.newArrayList(0, 1, 2, 7, 8, 9, 10, 14, 15, 16, 17)), tx);
            ruleUnification(qs.get(6), qs, subListExcluding(qs, Lists.newArrayList(4, 5, 6, 12, 16, 18)), tx);

            ruleUnification(qs.get(7), qs, subListExcluding(qs, Lists.newArrayList(9, 11, 12, 13, 18)), tx);
            ruleUnification(qs.get(8), qs, subListExcluding(qs, Lists.newArrayList(9, 10, 11, 12, 13, 18)), tx);
            ruleUnification(qs.get(9), qs, subListExcluding(qs, Lists.newArrayList(7, 8, 9, 10, 17)), tx);
            ruleUnification(qs.get(10), qs, subListExcluding(qs, Lists.newArrayList(8, 9, 10, 11, 12, 13, 17, 18)), tx);

            ruleUnification(qs.get(11), qs, subListExcluding(qs, Lists.newArrayList(5, 7, 8, 10, 16, 17)), tx);
            ruleUnification(qs.get(12), qs, subListExcluding(qs, Lists.newArrayList(5, 6, 7, 8, 10, 13, 16, 17)), tx);
            ruleUnification(qs.get(13), qs, subListExcluding(qs, Lists.newArrayList(4, 5, 7, 8, 10, 12, 16, 17, 18)), tx);

            ruleUnification(qs.get(14), qs, subListExcluding(qs, Lists.newArrayList(15)), tx);
            ruleUnification(qs.get(15), qs, subListExcluding(qs, Lists.newArrayList(14, 16, 17, 18)), tx);

            ruleUnification(qs.get(16), qs, subListExcluding(qs, Lists.newArrayList(3, 4, 6, 11, 12, 13, 15, 18)), tx);
            ruleUnification(qs.get(17), qs, subListExcluding(qs, Lists.newArrayList(9, 10, 11, 12, 13, 15, 18)), tx);
            ruleUnification(qs.get(18), qs, subListExcluding(qs, Lists.newArrayList(5, 6, 7, 8, 10, 13, 15, 16, 17)), tx);
        }
    }

    @Test
    public void testUnification_differentTypeVariants_EXACT(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            List<String> qs = TestQueryPattern.differentTypeVariants.patternList(resource, anotherResource);
            subListExcluding(qs, Lists.newArrayList(3, 4, 7, 8)).forEach(q -> exactUnification(q, qs, new ArrayList<>(), tx));
            exactUnification(qs.get(3), qs, Collections.singletonList(qs.get(4)), tx);
            exactUnification(qs.get(4), qs, Collections.singletonList(qs.get(3)), tx);
            exactUnification(qs.get(7), qs, subList(qs, Lists.newArrayList(8)), tx);
            exactUnification(qs.get(8), qs, subList(qs, Lists.newArrayList(7)), tx);
        }
    }

    @Test
    public void testUnification_differentTypeVariants_STRUCTURAL(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            List<String> qs = TestQueryPattern.differentTypeVariants.patternList(resource, anotherResource);
            subListExcluding(qs, Lists.newArrayList(3, 4, 6, 7, 8)).forEach(q -> structuralUnification(q, qs, new ArrayList<>(), tx));

            structuralUnification(qs.get(3), qs, Collections.singletonList(qs.get(4)), tx);
            structuralUnification(qs.get(4), qs, Collections.singletonList(qs.get(3)), tx);

            structuralUnification(qs.get(6), qs, subList(qs, Lists.newArrayList(7, 8)), tx);
            structuralUnification(qs.get(7), qs, subList(qs, Lists.newArrayList(6, 8)), tx);
            structuralUnification(qs.get(8), qs, subList(qs, Lists.newArrayList(6, 7)), tx);
        }
    }

    @Test
    public void testUnification_differentTypeVariants_RULE(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            List<String> qs = TestQueryPattern.differentTypeVariants.patternList(resource, anotherResource);

            ruleUnification(qs.get(0), qs, qs, tx);
            ruleUnification(qs.get(1), qs, subListExcluding(qs, Lists.newArrayList(3, 4, 13, 14, 15, 16, 17, 18)), tx);
            ruleUnification(qs.get(2), qs, subListExcluding(qs, Lists.newArrayList(3, 4, 13, 14, 15, 16, 17, 18)), tx);
            ruleUnification(qs.get(3), qs, subListExcluding(qs, Lists.newArrayList(1, 2, 6, 7, 8, 9, 10, 11, 12)), tx);
            ruleUnification(qs.get(4), qs, subListExcluding(qs, Lists.newArrayList(1, 2, 6, 7, 8, 9, 10, 11, 12)), tx);
            ruleUnification(qs.get(5), qs, qs, tx);

            ruleUnification(qs.get(6), qs, subList(qs, Lists.newArrayList(0, 1, 2, 5, 9, 10, 11, 12)), tx);
            ruleUnification(qs.get(7), qs, subList(qs, Lists.newArrayList(0, 1, 2, 5, 8, 9, 10, 11, 12)), tx);
            ruleUnification(qs.get(8), qs, subList(qs, Lists.newArrayList(0, 1, 2, 5, 7, 9, 10, 11, 12)), tx);

            ruleUnification(qs.get(9), qs, subList(qs, Lists.newArrayList(0, 1, 2, 5, 6, 7, 8, 11)), tx);
            ruleUnification(qs.get(10), qs, subList(qs, Lists.newArrayList(0, 1, 2, 5, 6, 7, 8, 11, 12)), tx);

            ruleUnification(qs.get(11), qs, subList(qs, Lists.newArrayList(0, 1, 2, 5, 6, 7, 8, 9, 10, 12)), tx);
            ruleUnification(qs.get(12), qs, subList(qs, Lists.newArrayList(0, 1, 2, 5, 6, 7, 8, 10, 11)), tx);

            ruleUnification(qs.get(13), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 16, 17, 18)), tx);
            ruleUnification(qs.get(14), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 15, 17, 18)), tx);

            ruleUnification(qs.get(15), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 14, 16, 17, 18)), tx);
            ruleUnification(qs.get(16), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 13, 15, 17, 18)), tx);
            ruleUnification(qs.get(17), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 13, 14, 15, 16, 18)), tx);
            ruleUnification(qs.get(18), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 13, 14, 15, 16, 17)), tx);
        }
    }

    @Test
    public void testUnification_differentResourceVariants_EXACT(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            List<String> qs = TestQueryPattern.differentResourceVariants.patternList(entity, anotherEntity, resource, anotherResource);

            exactUnification(qs.get(0), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(1), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(2), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(3), qs, new ArrayList<>(), tx);

            exactUnification(qs.get(4), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(5), qs, new ArrayList<>(), tx);

            exactUnification(qs.get(6), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(7), qs, new ArrayList<>(), tx);

            exactUnification(qs.get(8), qs, Collections.singletonList(qs.get(10)), tx);
            exactUnification(qs.get(9), qs, Collections.singletonList(qs.get(11)), tx);
            exactUnification(qs.get(10), qs, Collections.singletonList(qs.get(8)), tx);
            exactUnification(qs.get(11), qs, Collections.singletonList(qs.get(9)), tx);

            exactUnification(qs.get(12), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(13), qs, new ArrayList<>(), tx);

            exactUnification(qs.get(14), qs, Collections.singletonList(qs.get(16)), tx);
            exactUnification(qs.get(15), qs, Collections.singletonList(qs.get(17)), tx);
            exactUnification(qs.get(16), qs, Collections.singletonList(qs.get(14)), tx);
            exactUnification(qs.get(17), qs, Collections.singletonList(qs.get(15)), tx);

            exactUnification(qs.get(18), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(19), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(20), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(21), qs, new ArrayList<>(), tx);

            exactUnification(qs.get(22), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(23), qs, Collections.singletonList(qs.get(24)), tx);
            exactUnification(qs.get(24), qs, Collections.singletonList(qs.get(23)), tx);

            exactUnification(qs.get(25), qs, subList(qs, Lists.newArrayList(26)), tx);
            exactUnification(qs.get(26), qs, subList(qs, Lists.newArrayList(25)), tx);

            exactUnification(qs.get(27), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(28), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(29), qs, new ArrayList<>(), tx);
            exactUnification(qs.get(30), qs, new ArrayList<>(), tx);
        }
    }

    @Test
    public void testUnification_differentResourceVariants_STRUCTURAL(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            List<String> qs = TestQueryPattern.differentResourceVariants.patternList(entity, anotherEntity, resource, anotherResource);

            structuralUnification(qs.get(0), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(1), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(2), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(3), qs, new ArrayList<>(), tx);

            structuralUnification(qs.get(4), qs, Collections.singletonList(qs.get(5)), tx);
            structuralUnification(qs.get(5), qs, Collections.singletonList(qs.get(4)), tx);

            structuralUnification(qs.get(6), qs, Collections.singletonList(qs.get(7)), tx);
            structuralUnification(qs.get(7), qs, Collections.singletonList(qs.get(6)), tx);

            structuralUnification(qs.get(8), qs, Collections.singletonList(qs.get(10)), tx);
            structuralUnification(qs.get(9), qs, Collections.singletonList(qs.get(11)), tx);
            structuralUnification(qs.get(10), qs, Collections.singletonList(qs.get(8)), tx);
            structuralUnification(qs.get(11), qs, Collections.singletonList(qs.get(9)), tx);

            structuralUnification(qs.get(12), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(13), qs, new ArrayList<>(), tx);

            structuralUnification(qs.get(14), qs, Collections.singletonList(qs.get(16)), tx);
            structuralUnification(qs.get(15), qs, Collections.singletonList(qs.get(17)), tx);
            structuralUnification(qs.get(16), qs, Collections.singletonList(qs.get(14)), tx);
            structuralUnification(qs.get(17), qs, Collections.singletonList(qs.get(15)), tx);

            structuralUnification(qs.get(18), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(19), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(20), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(21), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(22), qs, new ArrayList<>(), tx);

            structuralUnification(qs.get(23), qs, Collections.singletonList(qs.get(24)), tx);
            structuralUnification(qs.get(24), qs, Collections.singletonList(qs.get(23)), tx);
            structuralUnification(qs.get(25), qs, subList(qs, Lists.newArrayList(26, 29)), tx);
            structuralUnification(qs.get(26), qs, subList(qs, Lists.newArrayList(25, 29)), tx);
            structuralUnification(qs.get(27), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(28), qs, new ArrayList<>(), tx);
            structuralUnification(qs.get(29), qs, subList(qs, Lists.newArrayList(25, 26)), tx);
            structuralUnification(qs.get(30), qs, new ArrayList<>(), tx);
        }
    }

    @Test
    public void testUnification_differentResourceVariants_RULE(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            List<String> qs = TestQueryPattern.differentResourceVariants.patternList(entity, anotherEntity, resource, anotherResource);

            ruleUnification(qs.get(0), qs, subList(qs, Lists.newArrayList(3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)), tx);
            ruleUnification(qs.get(1), qs, subListExcluding(qs, Lists.newArrayList(0, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 28)), tx);
            ruleUnification(qs.get(2), qs, subListExcluding(qs, Lists.newArrayList(0, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 28, 30)), tx);
            ruleUnification(qs.get(3), qs, subListExcluding(qs, Lists.newArrayList(28)), tx);

            ruleUnification(qs.get(4), qs, subList(qs, Lists.newArrayList(0, 3, 6, 7, 8, 9, 10, 11, 12, 13)), tx);
            ruleUnification(qs.get(5), qs, subList(qs, Lists.newArrayList(0, 3, 6, 7, 8, 9, 10, 11, 12, 13)), tx);

            ruleUnification(qs.get(6), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 8, 9, 10, 11, 12, 13)), tx);
            ruleUnification(qs.get(7), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 8, 9, 10, 11, 12, 13)), tx);

            ruleUnification(qs.get(8), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 6, 7, 10, 12)), tx);
            ruleUnification(qs.get(9), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 6, 7, 11, 12, 13)), tx);
            ruleUnification(qs.get(10), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 6, 7, 8, 12)), tx);
            ruleUnification(qs.get(11), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 6, 7, 9, 12, 13)), tx);

            ruleUnification(qs.get(12), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13)), tx);
            ruleUnification(qs.get(13), qs, subList(qs, Lists.newArrayList(0, 3, 4, 5, 6, 7, 9, 11, 12 )), tx);

            ruleUnification(qs.get(14), qs, subList(qs, Lists.newArrayList(1, 2, 3, 16, 19, 20, 21)), tx);
            ruleUnification(qs.get(15), qs, subList(qs, Lists.newArrayList(1, 2, 3, 17, 18, 20, 21)), tx);
            ruleUnification(qs.get(16), qs, subList(qs, Lists.newArrayList(1, 2, 3, 14, 19, 20, 21)), tx);
            ruleUnification(qs.get(17), qs, subList(qs, Lists.newArrayList(1, 2, 3, 15, 18, 20, 21)), tx);

            ruleUnification(qs.get(18), qs, subList(qs, Lists.newArrayList(1, 2, 3, 15, 17, 19, 20, 21, 22, 23, 24, 25, 26, 27, 29, 30)), tx);
            ruleUnification(qs.get(19), qs, subList(qs, Lists.newArrayList(1, 2, 3, 14, 16, 18, 20, 21)), tx);
            ruleUnification(qs.get(20), qs, subList(qs, Lists.newArrayList(1, 2, 3, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24, 25, 26, 27, 29, 30)), tx);
            ruleUnification(qs.get(21), qs, subList(qs, Lists.newArrayList(1, 2, 3, 14, 15, 16, 17, 18, 19, 20)), tx);

            ruleUnification(qs.get(22), qs, subList(qs, Lists.newArrayList(1, 2, 3, 18, 20, 22, 23, 24, 25, 26, 29, 30)), tx);
            ruleUnification(qs.get(23), qs, subList(qs, Lists.newArrayList(1, 2, 3, 18, 20, 22, 23, 24, 25, 26, 29)), tx);
            ruleUnification(qs.get(24), qs, subList(qs, Lists.newArrayList(1, 2, 3, 18, 20, 22, 23, 24, 25, 26, 29)), tx);

            ruleUnification(qs.get(25), qs, subList(qs, Lists.newArrayList(1, 2, 3, 18, 20, 22, 23, 24, 25, 26, 27, 29)), tx);
            ruleUnification(qs.get(26), qs, subList(qs, Lists.newArrayList(1, 2, 3, 18, 20, 22, 23, 24, 25, 26, 27, 29)), tx);

            ruleUnification(qs.get(27), qs, subList(qs, Lists.newArrayList(1, 2, 3, 18, 20, 25, 26, 29)), tx);
            ruleUnification(qs.get(28), subListExcluding(qs, Lists.newArrayList(28)), new ArrayList<>(), tx);
            ruleUnification(qs.get(29), qs, subList(qs, Lists.newArrayList(1, 2, 3, 18, 20, 22, 23, 24, 25, 26, 27, 29)), tx);
            ruleUnification(qs.get(30), qs, subList(qs, Lists.newArrayList(1, 3, 18, 20, 22)), tx);
        }
    }

    @Test
    public void testUnification_orthogonalityOfVariants_EXACT(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            List<List<String>> queryTypes = Lists.newArrayList(
                    TestQueryPattern.differentRelationVariants.patternList(entity, anotherBaseEntity, subEntity),
                    TestQueryPattern.differentRelationVariantsWithRelationVariable.patternList(entity, anotherBaseEntity, subEntity, relation, anotherRelation),
                    TestQueryPattern.differentTypeVariants.patternList(resource, anotherResource),
                    TestQueryPattern.differentResourceVariants.patternList(entity, anotherEntity, resource, anotherResource)
            );
            queryTypes.forEach(qt -> subListExcludingElements(queryTypes, Collections.singletonList(qt)).forEach(qto -> qt.forEach(q -> exactUnification(q, qto, new ArrayList<>(), tx))));
        }
    }

    @Test
    public void testUnification_orthogonalityOfVariants_STRUCTURAL(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            List<List<String>> queryTypes = Lists.newArrayList(
                    TestQueryPattern.differentRelationVariants.patternList(entity, anotherBaseEntity, subEntity),
                    TestQueryPattern.differentRelationVariantsWithRelationVariable.patternList(entity, anotherBaseEntity, subEntity, relation, anotherRelation),
                    TestQueryPattern.differentTypeVariants.patternList(resource, anotherResource),
                    TestQueryPattern.differentResourceVariants.patternList(entity, anotherEntity, resource, anotherResource)
            );
            queryTypes.forEach(qt -> subListExcludingElements(queryTypes, Collections.singletonList(qt)).forEach(qto -> qt.forEach(q -> structuralUnification(q, qto, new ArrayList<>(), tx))));
        }
    }

    private void unification(String child, List<String> queries, List<String> queriesWithUnifier, UnifierType unifierType, TransactionImpl tx){
        queries.forEach(parent -> unification(child, parent, queriesWithUnifier.contains(parent) || parent.equals(child), unifierType, tx));
    }

    private void structuralUnification(String child, List<String> queries, List<String> queriesWithUnifier, TransactionImpl tx){
        unification(child, queries, queriesWithUnifier, UnifierType.STRUCTURAL, tx);
    }

    private void exactUnification(String child, List<String> queries, List<String> queriesWithUnifier, TransactionImpl tx){
        unification(child, queries, queriesWithUnifier, UnifierType.EXACT, tx);
    }

    private void ruleUnification(String child, List<String> queries, List<String> queriesWithUnifier, TransactionImpl tx){
        unification(child, queries, queriesWithUnifier, UnifierType.RULE, tx);
    }


    private MultiUnifier unification(String childString, String parentString, boolean unifierExists, UnifierType unifierType, TransactionImpl tx){
        ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childString), tx);
        ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentString), tx);

        if (unifierType.equivalence() != null) queryEquivalence(child, parent, unifierExists, unifierType.equivalence());
        MultiUnifier multiUnifier = child.getMultiUnifier(parent, unifierType);
        assertEquals("Unexpected unifier: " + multiUnifier + " between the child - parent pair:\n" + child + " :\n" + parent, unifierExists, !multiUnifier.isEmpty());
        if (unifierExists && unifierType != UnifierType.RULE){
            MultiUnifier multiUnifierInverse = parent.getMultiUnifier(child, unifierType);
            assertEquals("Unexpected unifier inverse: " + multiUnifier + " between the child - parent pair:\n" + parent + " :\n" + child, unifierExists, !multiUnifierInverse.isEmpty());
            assertEquals(multiUnifierInverse, multiUnifier.inverse());
        }
        return multiUnifier;
    }

    /**
     * checks the correctness and uniqueness of an EXACT unifier required to unify child query with parent
     * @param parentString parent query string
     * @param childString child query string
     * @param checkInverse flag specifying whether the inverse equality u^{-1}=u(parent, child) of the unifier u(child, parent) should be checked
     * @param ignoreTypes flag specifying whether the types should be disregarded and only role players checked for containment
     * @param checkEquality if true the parent and child answers will be checked for equality, otherwise they are checked for containment of child answers in parent
     */
    private void unificationWithResultChecks(String parentString, String childString, boolean checkInverse, boolean checkEquality, boolean ignoreTypes, UnifierType unifierType, TransactionImpl<?> tx){
        ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childString), tx);
        ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentString), tx);
        Unifier unifier = unification(childString, parentString, true, unifierType, tx).getUnifier();

        List<ConceptMap> childAnswers = child.getQuery().execute();
        List<ConceptMap> unifiedAnswers = childAnswers.stream()
                .map(a -> a.unify(unifier))
                .filter(a -> !a.isEmpty())
                .collect(Collectors.toList());
        List<ConceptMap> parentAnswers = parent.getQuery().execute();

        if (checkInverse) {
            Unifier inverse = parent.getMultiUnifier(child, unifierType).getUnifier();
            assertEquals(unifier.inverse(), inverse);
            assertEquals(unifier, inverse.inverse());
        }

        assertTrue(!childAnswers.isEmpty());
        assertTrue(!unifiedAnswers.isEmpty());
        assertTrue(!parentAnswers.isEmpty());

        Set<Var> parentNonTypeVariables = Sets.difference(parent.getAtom().getVarNames(), Sets.newHashSet(parent.getAtom().getPredicateVariable()));
        if (!checkEquality){
            if(!ignoreTypes){
                assertTrue(parentAnswers.containsAll(unifiedAnswers));
            } else {
                List<ConceptMap> projectedParentAnswers = parentAnswers.stream().map(ans -> ans.project(parentNonTypeVariables)).collect(Collectors.toList());
                List<ConceptMap> projectedUnified = unifiedAnswers.stream().map(ans -> ans.project(parentNonTypeVariables)).collect(Collectors.toList());
                assertTrue(projectedParentAnswers.containsAll(projectedUnified));
            }

        } else {
            Unifier inverse = unifier.inverse();
            if(!ignoreTypes) {
                assertCollectionsEqual(parentAnswers, unifiedAnswers);
                List<ConceptMap> parentToChild = parentAnswers.stream().map(a -> a.unify(inverse)).collect(Collectors.toList());
                assertCollectionsEqual(parentToChild, childAnswers);
            } else {
                Set<Var> childNonTypeVariables = Sets.difference(child.getAtom().getVarNames(), Sets.newHashSet(child.getAtom().getPredicateVariable()));
                List<ConceptMap> projectedParentAnswers = parentAnswers.stream().map(ans -> ans.project(parentNonTypeVariables)).collect(Collectors.toList());
                List<ConceptMap> projectedUnified = unifiedAnswers.stream().map(ans -> ans.project(parentNonTypeVariables)).collect(Collectors.toList());
                List<ConceptMap> projectedChild = childAnswers.stream().map(ans -> ans.project(childNonTypeVariables)).collect(Collectors.toList());

                assertCollectionsEqual(projectedParentAnswers, projectedUnified);
                List<ConceptMap> projectedParentToChild = projectedParentAnswers.stream()
                        .map(a -> a.unify(inverse))
                        .map(ans -> ans.project(childNonTypeVariables))
                        .collect(Collectors.toList());
                assertCollectionsEqual(projectedParentToChild, projectedChild);
            }
        }
    }

    private static <T> void assertCollectionsEqual(Collection<T> c1, Collection<T> c2) {
        assertTrue(isEqualCollection(c1, c2));
    }


    private void queryEquivalence(ReasonerAtomicQuery a, ReasonerAtomicQuery b, boolean queryExpectation, ReasonerQueryEquivalence equiv){
        singleQueryEquivalence(a, a, true, equiv);
        singleQueryEquivalence(b, b, true, equiv);
        singleQueryEquivalence(a, b, queryExpectation, equiv);
        singleQueryEquivalence(b, a, queryExpectation, equiv);
    }

    private void singleQueryEquivalence(ReasonerAtomicQuery a, ReasonerAtomicQuery b, boolean queryExpectation, ReasonerQueryEquivalence equiv){
        assertEquals(equiv.name() + " - Query: " + a.toString() + " =? " + b.toString(), queryExpectation, equiv.equivalent(a, b));

        //check hash additionally if need to be equal
        if (queryExpectation) {
            assertEquals(a.toString() + " hash=? " + b.toString(), true, equiv.hash(a) == equiv.hash(b));
        }
    }

    private Concept getConceptByResourceValue(TransactionImpl<?> tx, String id){
        Set<Concept> instances = tx.getAttributesByValue(id)
                .stream().flatMap(Attribute::owners).collect(Collectors.toSet());
        if (instances.size() != 1)
            throw new IllegalStateException("Something wrong, multiple instances with given res value");
        return instances.iterator().next();
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString){
        Set<VarPatternAdmin> vars = Graql.parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}