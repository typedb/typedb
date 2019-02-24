/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.reasoner.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.Concept;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryEquivalence;
import grakn.core.graql.internal.reasoner.unifier.MultiUnifier;
import grakn.core.graql.internal.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.internal.reasoner.unifier.Unifier;
import grakn.core.graql.internal.reasoner.unifier.UnifierType;
import grakn.core.graql.reasoner.graph.GenericSchemaGraph;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.graql.reasoner.pattern.QueryPattern.subListExcludingElements;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.collections.CollectionUtils.isEqualCollection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class AtomicQueryUnificationIT {

    private static String resourcePath = "test-integration/graql/reasoner/resources/";

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl genericSchemaSession;
    private static SessionImpl unificationWithTypesSession;
    private static GenericSchemaGraph genericSchemaGraph;

    @BeforeClass
    public static void loadContext(){
        genericSchemaSession = server.sessionWithNewKeyspace();
        genericSchemaGraph = new GenericSchemaGraph(genericSchemaSession);
        unificationWithTypesSession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit(resourcePath,"unificationWithTypesTest.gql", unificationWithTypesSession);
    }

    @AfterClass
    public static void closeSession(){
        genericSchemaSession.close();
        unificationWithTypesSession.close();
    }

    @Test
    public void testUnification_RULE_BinaryRelationWithSubs(){
        try(TransactionOLTP tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {

            Concept x1 = getConceptByResourceValue(tx, "x1");
            Concept x2 = getConceptByResourceValue(tx, "x2");

            ReasonerAtomicQuery xbaseQuery = ReasonerQueries.atomic(conjunction("{ ($x1, $x2) isa binary; };"), tx);
            ReasonerAtomicQuery ybaseQuery = ReasonerQueries.atomic(conjunction("{ ($y1, $y2) isa binary; };"), tx);

            ConceptMap xAnswer = new ConceptMap(ImmutableMap.of(new Variable("x1"), x1, new Variable("x2"), x2));
            ConceptMap flippedXAnswer = new ConceptMap(ImmutableMap.of(new Variable("x1"), x2, new Variable("x2"), x1));

            ConceptMap yAnswer = new ConceptMap(ImmutableMap.of(new Variable("y1"), x1, new Variable("y2"), x2));
            ConceptMap flippedYAnswer = new ConceptMap(ImmutableMap.of(new Variable("y1"), x2, new Variable("y2"), x1));

            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(xbaseQuery, xAnswer);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(xbaseQuery, flippedXAnswer);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    new Variable("x1"), new Variable("x2"),
                    new Variable("x2"), new Variable("x1")
            ));
            assertEquals(correctUnifier, unifier);

            ReasonerAtomicQuery yChildQuery = ReasonerQueries.atomic(ybaseQuery, yAnswer);
            ReasonerAtomicQuery yChildQuery2 = ReasonerQueries.atomic(ybaseQuery, flippedYAnswer);

            MultiUnifier unifier2 = yChildQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier2 = new MultiUnifierImpl(ImmutableMultimap.of(
                    new Variable("y1"), new Variable("x1"),
                    new Variable("y2"), new Variable("x2")
            ));
            assertEquals(correctUnifier2, unifier2);

            MultiUnifier unifier3 = yChildQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier3 = new MultiUnifierImpl(ImmutableMultimap.of(
                    new Variable("y1"), new Variable("x2"),
                    new Variable("y2"), new Variable("x1")
            ));
            assertEquals(correctUnifier3, unifier3);
        }
    }

    @Test //only a single unifier exists
    public void testUnification_EXACT_BinaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        try(TransactionOLTP tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{ $x1 isa twoRoleEntity;($x1, $x2) isa binary; };"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{ $y1 isa twoRoleEntity;($y1, $y2) isa binary; };"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    new Variable("y1"), new Variable("x1"),
                    new Variable("y2"), new Variable("x2")
            ));
            assertTrue(unifier.equals(correctUnifier));
        }
    }

    @Test //only a single unifier exists
    public void testUnification_EXACT_BinaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes(){
        try(TransactionOLTP tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{ $x1 isa twoRoleEntity;$x2 isa twoRoleEntity2;($x1, $x2) isa binary; };"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{ $y1 isa twoRoleEntity;$y2 isa twoRoleEntity2;($y1, $y2) isa binary; };"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    new Variable("y1"), new Variable("x1"),
                    new Variable("y2"), new Variable("x2")
            ));
            assertEquals(correctUnifier, unifier);
        }
    }

    @Test
    public void testUnification_EXACT_TernaryRelation_ParentRepeatsRoles(){
        try(TransactionOLTP tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{ (role1: $x, role1: $y, role2: $z) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{ (role1: $u, role2: $v, role3: $q) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{ (role1: $u, role2: $v, role2: $q) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{ (role1: $u, role1: $v, role2: $q) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{ (role1: $u, role1: $u, role2: $q) isa ternary; };"), tx);

            MultiUnifier emptyUnifier = childQuery.getMultiUnifier(parentQuery);
            MultiUnifier emptyUnifier2 = childQuery2.getMultiUnifier(parentQuery);
            MultiUnifier emptyUnifier3 = childQuery4.getMultiUnifier(parentQuery);

            assertEquals(MultiUnifierImpl.nonExistent(), emptyUnifier);
            assertEquals(MultiUnifierImpl.nonExistent(), emptyUnifier2);
            assertEquals(MultiUnifierImpl.nonExistent(), emptyUnifier3);

            MultiUnifier unifier = childQuery3.getMultiUnifier(parentQuery);
            MultiUnifier correctUnifier = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            new Variable("u"), new Variable("x"),
                            new Variable("v"), new Variable("y"),
                            new Variable("q"), new Variable("z")),
                    ImmutableMultimap.of(
                            new Variable("u"), new Variable("y"),
                            new Variable("v"), new Variable("x"),
                            new Variable("q"), new Variable("z"))
            );
            assertEquals(correctUnifier, unifier);
            assertEquals(2, unifier.size());
        }
    }

    @Test
    public void testUnification_EXACT_TernaryRelation_ParentRepeatsMetaRoles_ParentRepeatsRPs(){
        try(TransactionOLTP tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{ (role: $x, role: $x, role2: $y) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{ (role1: $u, role2: $v, role3: $q) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{ (role1: $u, role2: $v, role2: $q) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{ (role1: $u, role1: $v, role2: $q) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{ (role1: $u, role1: $u, role2: $q) isa ternary; };"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            new Variable("q"), new Variable("x"),
                            new Variable("u"), new Variable("x"),
                            new Variable("v"), new Variable("y"))
            );
            assertEquals(correctUnifier, unifier);

            MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier2 = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            new Variable("u"), new Variable("x"),
                            new Variable("q"), new Variable("x"),
                            new Variable("v"), new Variable("y")),
                    ImmutableMultimap.of(
                            new Variable("u"), new Variable("x"),
                            new Variable("v"), new Variable("x"),
                            new Variable("q"), new Variable("y"))
            );
            assertEquals(correctUnifier2, unifier2);
            assertEquals(unifier2.size(), 2);

            MultiUnifier unifier3 = childQuery3.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier3 = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            new Variable("u"), new Variable("x"),
                            new Variable("v"), new Variable("x"),
                            new Variable("q"), new Variable("y"))
            );
            assertEquals(correctUnifier3, unifier3);

            MultiUnifier unifier4 = childQuery4.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier4 = new MultiUnifierImpl(ImmutableMultimap.of(
                    new Variable("u"), new Variable("x"),
                    new Variable("q"), new Variable("y")
            ));
            assertEquals(correctUnifier4, unifier4);
        }
    }

    @Test
    public void testUnification_EXACT_TernaryRelationWithTypes_SomeVarsHaveTypes_UnifierMatchesTypes(){
        try(TransactionOLTP tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{ $x1 isa threeRoleEntity;$x3 isa threeRoleEntity3;($x1, $x2, $x3) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{ $y3 isa threeRoleEntity3;$y1 isa threeRoleEntity;($y2, $y3, $y1) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{ $y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary; };"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.EXACT);
            MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.EXACT);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    new Variable("y1"), new Variable("x1"),
                    new Variable("y2"), new Variable("x2"),
                    new Variable("y3"), new Variable("x3")
            ));
            assertEquals(correctUnifier, unifier);
            assertEquals(MultiUnifierImpl.nonExistent(), unifier2);
        }
    }

    @Test
    public void testUnification_RULE_TernaryRelation_ParentRepeatsMetaRoles(){
        try(TransactionOLTP tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{ (role: $x, role: $y, role2: $z) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{ (role1: $u, role2: $v, role3: $q) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{ (role1: $u, role2: $v, role2: $q) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{ (role1: $u, role1: $v, role2: $q) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{ (role1: $u, role1: $u, role2: $q) isa ternary; };"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            new Variable("u"), new Variable("x"),
                            new Variable("v"), new Variable("z"),
                            new Variable("q"), new Variable("y")),
                    ImmutableMultimap.of(
                            new Variable("u"), new Variable("y"),
                            new Variable("v"), new Variable("z"),
                            new Variable("q"), new Variable("x"))
            );
            assertEquals(correctUnifier, unifier);
            assertEquals(2, unifier.size());

            MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier2 = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            new Variable("u"), new Variable("x"),
                            new Variable("v"), new Variable("y"),
                            new Variable("q"), new Variable("z")),
                    ImmutableMultimap.of(
                            new Variable("u"), new Variable("x"),
                            new Variable("v"), new Variable("z"),
                            new Variable("q"), new Variable("y")),
                    ImmutableMultimap.of(
                            new Variable("u"), new Variable("y"),
                            new Variable("v"), new Variable("z"),
                            new Variable("q"), new Variable("x")),
                    ImmutableMultimap.of(
                            new Variable("u"), new Variable("y"),
                            new Variable("v"), new Variable("x"),
                            new Variable("q"), new Variable("z"))
            );
            assertEquals(correctUnifier2, unifier2);
            assertEquals(4, unifier2.size());

            MultiUnifier unifier3 = childQuery3.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier3 = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            new Variable("u"), new Variable("x"),
                            new Variable("v"), new Variable("y"),
                            new Variable("q"), new Variable("z")),
                    ImmutableMultimap.of(
                            new Variable("u"), new Variable("y"),
                            new Variable("v"), new Variable("x"),
                            new Variable("q"), new Variable("z"))
            );
            assertEquals(correctUnifier3, unifier3);
            assertEquals(2, unifier3.size());

            MultiUnifier unifier4 = childQuery4.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier4 = new MultiUnifierImpl(ImmutableMultimap.of(
                    new Variable("u"), new Variable("x"),
                    new Variable("u"), new Variable("y"),
                    new Variable("q"), new Variable("z")
            ));
            assertEquals(correctUnifier4, unifier4);
        }
    }

    @Test
    public void testUnification_RULE_TernaryRelation_ParentRepeatsRoles_ParentRepeatsRPs(){
        try(TransactionOLTP tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{ (role1: $x, role1: $x, role2: $y) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{ (role1: $u, role2: $v, role3: $q) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{ (role1: $u, role2: $v, role2: $q) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery3 = ReasonerQueries.atomic(conjunction("{ (role1: $u, role1: $v, role2: $q) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery4 = ReasonerQueries.atomic(conjunction("{ (role1: $u, role1: $u, role2: $q) isa ternary; };"), tx);

            MultiUnifier emptyUnifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier emptyUnifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);

            assertTrue(emptyUnifier.isEmpty());
            assertTrue(emptyUnifier2.isEmpty());

            MultiUnifier unifier = childQuery3.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    new Variable("u"), new Variable("x"),
                    new Variable("v"), new Variable("x"),
                    new Variable("q"), new Variable("y")
            ));
            assertEquals(correctUnifier, unifier);

            MultiUnifier unifier2 = childQuery4.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier2 = new MultiUnifierImpl(ImmutableMultimap.of(
                    new Variable("u"), new Variable("x"),
                    new Variable("q"), new Variable("y")
            ));
            assertEquals(correctUnifier2, unifier2);
        }
    }

    @Test
    public void testUnification_RULE_TernaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes(){
        try(TransactionOLTP tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{ $x1 isa threeRoleEntity;$x2 isa threeRoleEntity2; $x3 isa threeRoleEntity3;($x1, $x2, $x3) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{ $y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;($y2, $y3, $y1) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{ $y3 isa threeRoleEntity3;$y2 isa threeRoleEntity2;$y1 isa threeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary; };"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(ImmutableMultimap.of(
                    new Variable("y1"), new Variable("x1"),
                    new Variable("y2"), new Variable("x2"),
                    new Variable("y3"), new Variable("x3")
            ));
            assertEquals(correctUnifier, unifier);
            assertEquals(correctUnifier, unifier2);
        }
    }

    @Test // subSubThreeRoleEntity sub subThreeRoleEntity sub threeRoleEntity3
    public void testUnification_RULE_TernaryRelationWithTypes_AllVarsHaveTypes_UnifierMatchesTypes_TypeHierarchyInvolved(){
        try(TransactionOLTP tx = unificationWithTypesSession.transaction(Transaction.Type.READ)) {
            ReasonerAtomicQuery parentQuery = ReasonerQueries.atomic(conjunction("{ $x1 isa threeRoleEntity;$x2 isa subThreeRoleEntity; $x3 isa subSubThreeRoleEntity;($x1, $x2, $x3) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery = ReasonerQueries.atomic(conjunction("{ $y1 isa threeRoleEntity;$y2 isa subThreeRoleEntity;$y3 isa subSubThreeRoleEntity;($y2, $y3, $y1) isa ternary; };"), tx);
            ReasonerAtomicQuery childQuery2 = ReasonerQueries.atomic(conjunction("{ $y1 isa threeRoleEntity;$y2 isa subThreeRoleEntity;$y3 isa subSubThreeRoleEntity;(role2: $y2, role3: $y3, role1: $y1) isa ternary; };"), tx);

            MultiUnifier unifier = childQuery.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier unifier2 = childQuery2.getMultiUnifier(parentQuery, UnifierType.RULE);
            MultiUnifier correctUnifier = new MultiUnifierImpl(
                    ImmutableMultimap.of(
                            new Variable("y1"), new Variable("x1"),
                            new Variable("y2"), new Variable("x2"),
                            new Variable("y3"), new Variable("x3")),
                    ImmutableMultimap.of(
                            new Variable("y1"), new Variable("x1"),
                            new Variable("y2"), new Variable("x3"),
                            new Variable("y3"), new Variable("x2")),
                    ImmutableMultimap.of(
                            new Variable("y1"), new Variable("x2"),
                            new Variable("y2"), new Variable("x1"),
                            new Variable("y3"), new Variable("x3")),
                    ImmutableMultimap.of(
                            new Variable("y1"), new Variable("x2"),
                            new Variable("y2"), new Variable("x3"),
                            new Variable("y3"), new Variable("x1")),
                    ImmutableMultimap.of(
                            new Variable("y1"), new Variable("x3"),
                            new Variable("y2"), new Variable("x1"),
                            new Variable("y3"), new Variable("x2")),
                    ImmutableMultimap.of(
                            new Variable("y1"), new Variable("x3"),
                            new Variable("y2"), new Variable("x2"),
                            new Variable("y3"), new Variable("x1"))
            );
            assertEquals(correctUnifier, unifier);
            assertEquals(correctUnifier, unifier2);
        }
    }


    @Test
    public void testUnification_RULE_ResourcesWithTypes(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            String parentQuery = "{ $x has resource $r; $x isa baseRoleEntity; };";

            String childQuery = "{ $r has resource $x; $r isa subRoleEntity; };";
            String childQuery2 = "{ $x1 has resource $x; $x1 isa subSubRoleEntity; };";
            String baseQuery = "{ $r has resource $x; $r isa entity; };";

            unificationWithResultChecks(parentQuery, childQuery, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentQuery, childQuery2, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentQuery, baseQuery, true, true, true, UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_RULE_BinaryRelationWithRoleAndTypeHierarchy_MetaTypeParent(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            String parentRelation = "{ (baseRole1: $x, baseRole2: $y); $x isa entity; $y isa entity; };";

            String specialisedRelation = "{ (subRole1: $u, anotherSubRole2: $v); $u isa baseRoleEntity; $v isa baseRoleEntity; };";
            String specialisedRelation2 = "{ (subRole1: $y, anotherSubRole2: $x); $y isa baseRoleEntity; $x isa baseRoleEntity; };";
            String specialisedRelation3 = "{ (subRole1: $u, anotherSubRole2: $v); $u isa subRoleEntity; $v isa subRoleEntity; };";
            String specialisedRelation4 = "{ (subRole1: $y, anotherSubRole2: $x); $y isa subRoleEntity; $x isa subRoleEntity; };";
            String specialisedRelation5 = "{ (subSubRole1: $u, subSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity; };";
            String specialisedRelation6 = "{ (subSubRole1: $y, subSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity; };";

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
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            String baseParentRelation = "{ (baseRole1: $x, baseRole2: $y); $x isa baseRoleEntity; $y isa baseRoleEntity; };";
            String parentRelation = "{ (baseRole1: $x, baseRole2: $y); $x isa subSubRoleEntity; $y isa subSubRoleEntity; };";

            String specialisedRelation = "{ (subRole1: $u, anotherSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity; };";
            String specialisedRelation2 = "{ (subRole1: $y, anotherSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity; };";
            String specialisedRelation3 = "{ (subSubRole1: $u, subSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity; };";
            String specialisedRelation4 = "{ (subSubRole1: $y, subSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity; };";

            unificationWithResultChecks(baseParentRelation, specialisedRelation, false, false, true, UnifierType.RULE, tx);
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
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            String parentRelation = "{ (baseRole1: $x, baseRole2: $y); $x isa subRoleEntity; $y isa subRoleEntity; };";

            String specialisedRelation = "{ (subRole1: $u, anotherSubRole2: $v); $u isa subRoleEntity; $v isa subSubRoleEntity; };";
            String specialisedRelation2 = "{ (subRole1: $y, anotherSubRole2: $x); $y isa subRoleEntity; $x isa subSubRoleEntity; };";
            String specialisedRelation3 = "{ (subSubRole1: $u, subSubRole2: $v); $u isa subSubRoleEntity; $v isa subSubRoleEntity; };";
            String specialisedRelation4 = "{ (subSubRole1: $y, subSubRole2: $x); $y isa subSubRoleEntity; $x isa subSubRoleEntity; };";

            unificationWithResultChecks(parentRelation, specialisedRelation, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation2, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation3, false, false, true, UnifierType.RULE, tx);
            unificationWithResultChecks(parentRelation, specialisedRelation4, false, false, true, UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariants_EXACT(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    genericSchemaGraph.differentRelationVariants().patterns(),
                    genericSchemaGraph.differentRelationVariants().exactMatrix(),
                    UnifierType.EXACT, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariants_STRUCTURAL(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    genericSchemaGraph.differentRelationVariants().patterns(),
                    genericSchemaGraph.differentRelationVariants().structuralMatrix(),
                    UnifierType.STRUCTURAL, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariants_RULE(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    genericSchemaGraph.differentRelationVariants().patterns(),
                    genericSchemaGraph.differentRelationVariants().ruleMatrix(),
                    UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariantsWithMetaRoles_EXACT(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    genericSchemaGraph.differentRelationVariantsWithMetaRoles().patterns(),
                    genericSchemaGraph.differentRelationVariantsWithMetaRoles().exactMatrix(),
                    UnifierType.EXACT, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariantsWithMetaRoles_STRUCTURAL(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    genericSchemaGraph.differentRelationVariantsWithMetaRoles().patterns(),
                    genericSchemaGraph.differentRelationVariantsWithMetaRoles().structuralMatrix(),
                    UnifierType.STRUCTURAL, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariantsWithMetaRoles_RULE(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(genericSchemaGraph.differentRelationVariantsWithMetaRoles().patterns(), genericSchemaGraph.differentRelationVariantsWithMetaRoles().ruleMatrix(), UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariantsWithRelationVariable_EXACT(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    genericSchemaGraph.differentRelationVariantsWithRelationVariable().patterns(),
                    genericSchemaGraph.differentRelationVariantsWithRelationVariable().exactMatrix(),
                    UnifierType.EXACT, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariantsWithRelationVariable_STRUCTURAL(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    genericSchemaGraph.differentRelationVariantsWithRelationVariable().patterns(),
                    genericSchemaGraph.differentRelationVariantsWithRelationVariable().structuralMatrix(),
                    UnifierType.STRUCTURAL, tx);
        }
    }

    @Test
    public void testUnification_differentRelationVariantsWithRelationVariable_RULE(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    genericSchemaGraph.differentRelationVariantsWithRelationVariable().patterns(),
                    genericSchemaGraph.differentRelationVariantsWithRelationVariable().ruleMatrix(),
                    UnifierType.RULE, tx);
        }
    }

    @Test
    public void testUnification_differentTypeVariants_EXACT(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            List<String> qs = genericSchemaGraph.differentTypeResourceVariants().patterns();
            qs.forEach(q -> exactUnification(q, qs, new ArrayList<>(), tx));
        }
    }

    @Test
    public void testUnification_differentTypeVariants_STRUCTURAL(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    genericSchemaGraph.differentTypeResourceVariants().patterns(),
                    genericSchemaGraph.differentTypeResourceVariants().patterns(),
                    genericSchemaGraph.differentTypeResourceVariants().structuralMatrix(),
                    UnifierType.STRUCTURAL,
                    tx
            );
        }
    }

    @Test
    public void testUnification_differentTypeVariants_RULE(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    genericSchemaGraph.differentTypeResourceVariants().patterns(),
                    genericSchemaGraph.differentTypeResourceVariants().patterns(),
                    genericSchemaGraph.differentTypeResourceVariants().ruleMatrix(),
                    UnifierType.RULE,
                    tx
            );
        }
    }

    @Test
    public void testUnification_differentResourceVariants_EXACT(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    genericSchemaGraph.differentResourceVariants().patterns(),
                    genericSchemaGraph.differentResourceVariants().patterns(),
                    genericSchemaGraph.differentResourceVariants().exactMatrix(),
                    UnifierType.EXACT,
                    tx);
        }
    }

    @Test
    public void testUnification_differentResourceVariants_STRUCTURAL(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    genericSchemaGraph.differentResourceVariants().patterns(),
                    genericSchemaGraph.differentResourceVariants().patterns(),
                    genericSchemaGraph.differentResourceVariants().structuralMatrix(),
                    UnifierType.STRUCTURAL,
                    tx);
        }
    }

    @Test
    public void testUnification_differentResourceVariants_RULE(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            unification(
                    genericSchemaGraph.differentResourceVariants().patterns(),
                    genericSchemaGraph.differentResourceVariants().patterns(),
                    genericSchemaGraph.differentResourceVariants().ruleMatrix(),
                    UnifierType.RULE,
                    tx);
        }
    }

    @Test
    public void testUnification_orthogonalityOfVariants_EXACT(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            List<List<String>> queryTypes = Lists.newArrayList(
                    genericSchemaGraph.differentRelationVariants().patterns(),
                    genericSchemaGraph.differentRelationVariantsWithRelationVariable().patterns(),
                    genericSchemaGraph.differentTypeResourceVariants().patterns(),
                    genericSchemaGraph.differentResourceVariants().patterns()
            );
            queryTypes.forEach(qt -> subListExcludingElements(queryTypes, Collections.singletonList(qt)).forEach(qto -> qt.forEach(q -> exactUnification(q, qto, new ArrayList<>(), tx))));
        }
    }

    @Test
    public void testUnification_orthogonalityOfVariants_STRUCTURAL(){
        try(TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.READ)) {
            List<List<String>> queryTypes = Lists.newArrayList(
                    genericSchemaGraph.differentRelationVariants().patterns(),
                    genericSchemaGraph.differentRelationVariantsWithRelationVariable().patterns(),
                    genericSchemaGraph.differentTypeResourceVariants().patterns(),
                    genericSchemaGraph.differentResourceVariants().patterns()
            );
            queryTypes.forEach(qt -> subListExcludingElements(queryTypes, Collections.singletonList(qt)).forEach(qto -> qt.forEach(q -> structuralUnification(q, qto, new ArrayList<>(), tx))));
        }
    }


    private void unification(String child, List<String> queries, List<String> queriesWithUnifier, UnifierType unifierType, TransactionOLTP tx){
        queries.forEach(parent -> unification(child, parent, queriesWithUnifier.contains(parent) || parent.equals(child), unifierType, tx));
    }

    private void unification(List<String> queries, int[][] resultMatrix, UnifierType unifierType, TransactionOLTP tx){
        unification(queries, queries, resultMatrix, unifierType, tx);
    }

    private void unification(List<String> children, List<String> parents, int[][] resultMatrix, UnifierType unifierType, TransactionOLTP tx){
        int i = 0;
        int j = 0;
        for (String child : children) {
            for (String parent : parents) {
                unification(child, parent, resultMatrix[i][j] == 1, unifierType, tx);
                j++;
            }
            i++;
            j = 0;
        }
    }

    private void structuralUnification(String child, List<String> queries, List<String> queriesWithUnifier, TransactionOLTP tx){
        unification(child, queries, queriesWithUnifier, UnifierType.STRUCTURAL, tx);
    }

    private void exactUnification(String child, List<String> queries, List<String> queriesWithUnifier, TransactionOLTP tx){
        unification(child, queries, queriesWithUnifier, UnifierType.EXACT, tx);
    }

    private MultiUnifier unification(String childString, String parentString, boolean unifierExists, UnifierType unifierType, TransactionOLTP tx){
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
    private void unificationWithResultChecks(String parentString, String childString, boolean checkInverse, boolean checkEquality, boolean ignoreTypes, UnifierType unifierType, TransactionOLTP tx){
        ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childString), tx);
        ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentString), tx);
        Unifier unifier = unification(childString, parentString, true, unifierType, tx).getUnifier();

        List<ConceptMap> childAnswers = tx.execute(child.getQuery(), false);
        List<ConceptMap> unifiedAnswers = childAnswers.stream()
                .map(unifier::apply)
                .filter(a -> !a.isEmpty())
                .collect(Collectors.toList());
        List<ConceptMap> parentAnswers = tx.execute(parent.getQuery(), false);

        if (checkInverse) {
            Unifier inverse = parent.getMultiUnifier(child, unifierType).getUnifier();
            assertEquals(unifier.inverse(), inverse);
            assertEquals(unifier, inverse.inverse());
        }

        assertTrue(!childAnswers.isEmpty());
        assertTrue(!unifiedAnswers.isEmpty());
        assertTrue(!parentAnswers.isEmpty());

        Set<Variable> parentNonTypeVariables = Sets.difference(parent.getAtom().getVarNames(), Sets.newHashSet(parent.getAtom().getPredicateVariable()));
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
                assertCollectionsNonTriviallyEqual(parentAnswers, unifiedAnswers);
                List<ConceptMap> parentToChild = parentAnswers.stream().map(inverse::apply).collect(Collectors.toList());
                assertCollectionsNonTriviallyEqual(parentToChild, childAnswers);
            } else {
                Set<Variable> childNonTypeVariables = Sets.difference(child.getAtom().getVarNames(), Sets.newHashSet(child.getAtom().getPredicateVariable()));
                List<ConceptMap> projectedParentAnswers = parentAnswers.stream().map(ans -> ans.project(parentNonTypeVariables)).collect(Collectors.toList());
                List<ConceptMap> projectedUnified = unifiedAnswers.stream().map(ans -> ans.project(parentNonTypeVariables)).collect(Collectors.toList());
                List<ConceptMap> projectedChild = childAnswers.stream().map(ans -> ans.project(childNonTypeVariables)).collect(Collectors.toList());

                assertCollectionsNonTriviallyEqual(projectedParentAnswers, projectedUnified);
                List<ConceptMap> projectedParentToChild = projectedParentAnswers.stream()
                        .map(inverse::apply)
                        .map(ans -> ans.project(childNonTypeVariables))
                        .collect(Collectors.toList());
                assertCollectionsNonTriviallyEqual(projectedParentToChild, projectedChild);
            }
        }
    }

    private static <T> void assertCollectionsNonTriviallyEqual(Collection<T> c1, Collection<T> c2) {
        assertTrue(isEqualCollection(c1, c2));
    }

    private void queryEquivalence(ReasonerAtomicQuery a, ReasonerAtomicQuery b, boolean queryExpectation, ReasonerQueryEquivalence equiv){
        singleQueryEquivalence(a, a, true, equiv);
        singleQueryEquivalence(b, b, true, equiv);
        singleQueryEquivalence(a, b, queryExpectation, equiv);
        singleQueryEquivalence(b, a, queryExpectation, equiv);
    }

    private void singleQueryEquivalence(ReasonerAtomicQuery a, ReasonerAtomicQuery b, boolean queryExpectation, ReasonerQueryEquivalence equiv){
        assertEquals(equiv.name() + " - Queries:\n" + a.toString() + "\n=?\n" + b.toString(), queryExpectation, equiv.equivalent(a, b));

        //check hash additionally if need to be equal
        if (queryExpectation) {
            assertEquals(equiv.name() + ": " + a.toString() + " hash=? " + b.toString(), equiv.hash(a), equiv.hash(b));
        }
    }

    private Concept getConceptByResourceValue(TransactionOLTP tx, String id){
        Set<Concept> instances = tx.getAttributesByValue(id)
                .stream().flatMap(Attribute::owners).collect(Collectors.toSet());
        if (instances.size() != 1) {
            throw new IllegalStateException("Something wrong, multiple instances with given res value");
        }
        return instances.iterator().next();
    }

    private Conjunction<Statement> conjunction(String patternString){
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}