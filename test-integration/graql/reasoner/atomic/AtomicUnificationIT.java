/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graql.reasoner.atomic;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import grakn.core.common.config.Config;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.graql.reasoner.rule.InferenceRule;
import grakn.core.graql.reasoner.unifier.UnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.util.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static graql.lang.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class AtomicUnificationIT {

    private static String resourcePath = "test-integration/graql/reasoner/resources/";

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session genericSchemaSession;

    private Transaction tx;
    // factories exposed by a test transaction, bound to the lifetime of a tx
    private ReasonerQueryFactory reasonerQueryFactory;

    @BeforeClass
    public static void loadContext(){
        Config mockServerConfig = storage.createCompatibleServerConfig();
        genericSchemaSession = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        loadFromFileAndCommit(resourcePath,"genericSchema.gql", genericSchemaSession);
    }

    @AfterClass
    public static void closeSession(){
        genericSchemaSession.close();
    }

    @Before
    public void setUp(){
        tx = genericSchemaSession.writeTransaction();
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction) tx);
        reasonerQueryFactory = testTx.reasonerQueryFactory();
    }

    @After
    public void tearDown(){
        tx.close();
    }

    @Test
    public void testUnification_RelationWithRolesExchanged(){
        String relation = "{ (baseRole1: $x, baseRole2: $y) isa binary; };";
        String relation2 = "{ (baseRole1: $y, baseRole2: $x) isa binary; };";
        unification(relation, relation2, UnifierType.EXACT,true, true, tx);
    }

    @Test
    public void testUnification_RelationWithMetaRole(){
        String relation = "{ (baseRole1: $x, role: $y) isa binary; };";
        String relation2 = "{ (baseRole1: $y, role: $x) isa binary; };";
        unification(relation, relation2, UnifierType.EXACT,true, true, tx);
    }

    @Test
    public void testUnification_RelationWithRelationVar(){
        String relation = "{ $x (baseRole1: $r, baseRole2: $z) isa binary; };";
        String relation2 = "{ $r (baseRole1: $x, baseRole2: $y) isa binary; };";
        unification(relation, relation2,UnifierType.EXACT, true, true, tx);
    }

    @Test
    public void testUnification_RelationWithMetaRolesAndIds(){
        Concept instance = tx.execute(Graql.parse("match $x isa subRoleEntity; get;").asGet()).iterator().next().get("x");
        String relation = "{ (role: $x, role: $y) isa binary; $y id " + instance.id().getValue() + "; };";
        String relation2 = "{ (role: $z, role: $v) isa binary; $z id " + instance.id().getValue() + "; };";
        String relation3 = "{ (role: $z, role: $v) isa binary; $v id " + instance.id().getValue() + "; };";

        unification(relation, relation2, UnifierType.EXACT,true, true, tx);
        unification(relation, relation3, UnifierType.EXACT, true, true, tx);
        unification(relation2, relation3, UnifierType.EXACT,true, true, tx);
    }

    @Test
    public void testUnification_BinaryRelationWithRoleHierarchy_ParentWithBaseRoles(){
        String parentRelation = "{ (baseRole1: $x, baseRole2: $y); };";
        String specialisedRelation = "{ (subRole1: $u, anotherSubRole2: $v); };";
        String specialisedRelation2 = "{ (subRole1: $y, anotherSubRole2: $x); };";
        String specialisedRelation3 = "{ (subSubRole1: $u, subSubRole2: $v); };";
        String specialisedRelation4 = "{ (subSubRole1: $y, subSubRole2: $x); };";
        String specialisedRelation5 = "{ (subRole1: $u, anotherSubRole1: $v); };";

        unification(parentRelation, specialisedRelation, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation2, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation3, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation4, UnifierType.RULE,false, false, tx);
        nonExistentUnifier(parentRelation, specialisedRelation5);
    }

    @Test
    public void testUnification_BinaryRelationWithRoleHierarchy_ParentWithSubRoles(){
        String parentRelation = "{ (subRole1: $x, subRole2: $y); };";
        String specialisedRelation = "{ (subRole1: $u, subSubRole2: $v); };";
        String specialisedRelation2 = "{ (subRole1: $y, subSubRole2: $x); };";
        String specialisedRelation3 = "{ (subSubRole1: $u, subSubRole2: $v); };";
        String specialisedRelation4 = "{ (subSubRole1: $y, subSubRole2: $x); };";
        String specialisedRelation5 = "{ (subSubRole1: $u, baseRole3: $v); };";
        String specialisedRelation6 = "{ (baseRole1: $u, baseRole2: $v); };";

        unification(parentRelation, specialisedRelation, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation2, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation3, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation4, UnifierType.RULE,false, false, tx);
        nonExistentUnifier(parentRelation, specialisedRelation5);
        nonExistentUnifier(parentRelation, specialisedRelation6);
    }

    @Test
    public void testUnification_TernaryRelationWithRoleHierarchy_ParentWithBaseRoles(){
        String parentRelation = "{ (baseRole1: $x, baseRole2: $y, baseRole3: $z); };";
        String specialisedRelation = "{ (baseRole1: $u, subRole2: $v, subSubRole3: $q); };";
        String specialisedRelation2 = "{ (baseRole1: $z, subRole2: $y, subSubRole3: $x); };";
        String specialisedRelation3 = "{ (subRole1: $u, subRole2: $v, subSubRole3: $q); };";
        String specialisedRelation4 = "{ (subRole1: $y, subRole2: $z, subSubRole3: $x); };";
        String specialisedRelation5 = "{ (subRole1: $u, subRole1: $v, subSubRole3: $q); };";

        unification(parentRelation, specialisedRelation, UnifierType.RULE,false, true, tx);
        unification(parentRelation, specialisedRelation2, UnifierType.RULE,false, true, tx);
        unification(parentRelation, specialisedRelation3, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation4, UnifierType.RULE,false, false, tx);
        nonExistentUnifier(parentRelation, specialisedRelation5);
    }

    @Test
    public void testUnification_TernaryRelationWithRoleHierarchy_ParentWithSubRoles(){
        String parentRelation = "{ (subRole1: $x, subRole2: $y, subRole3: $z); };";
        String specialisedRelation = "{ (baseRole1: $u, subRole2: $v, subSubRole3: $q); };";
        String specialisedRelation2 = "{ (subRole1: $u, subRole2: $v, subSubRole3: $q); };";
        String specialisedRelation3 = "{ (subRole1: $y, subRole2: $z, subSubRole3: $x); };";
        String specialisedRelation4 = "{ (subSubRole1: $u, subRole2: $v, subSubRole3: $q); };";
        String specialisedRelation5 = "{ (subSubRole1: $y, subRole2: $z, subSubRole3: $x); };";
        String specialisedRelation6 = "{ (subRole1: $u, subRole1: $v, subSubRole3: $q); };";

        nonExistentUnifier(parentRelation, specialisedRelation);
        unification(parentRelation, specialisedRelation2, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation3, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation4, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation5, UnifierType.RULE,false, false, tx);
        nonExistentUnifier(parentRelation, specialisedRelation6);
    }

    @Test
    public void testUnification_TernaryRelationWithRoleHierarchy_ParentWithBaseRoles_childrenRepeatRolePlayers(){
        String parentRelation = "{ (baseRole1: $x, baseRole2: $y, baseRole3: $z); };";
        String specialisedRelation = "{ (baseRole1: $u, subRole2: $u, subSubRole3: $q); };";
        String specialisedRelation2 = "{ (baseRole1: $y, subRole2: $y, subSubRole3: $x); };";
        String specialisedRelation3 = "{ (subRole1: $u, subRole2: $u, subSubRole3: $q); };";
        String specialisedRelation4 = "{ (subRole1: $y, subRole2: $y, subSubRole3: $x); };";
        String specialisedRelation5 = "{ (subRole1: $u, subRole1: $u, subSubRole3: $q); };";

        unification(parentRelation, specialisedRelation, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation2, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation3, UnifierType.RULE, false, false, tx);
        unification(parentRelation, specialisedRelation4, UnifierType.RULE,false, false, tx);
        nonExistentUnifier(parentRelation, specialisedRelation5);
    }

    @Test
    public void testUnification_TernaryRelationWithRoleHierarchy_ParentWithBaseRoles_parentRepeatRolePlayers(){
        String parentRelation = "{ (baseRole1: $x, baseRole2: $x, baseRole3: $y); };";
        String specialisedRelation = "{ (baseRole1: $u, subRole2: $v, subSubRole3: $q); };";
        String specialisedRelation2 = "{ (baseRole1: $z, subRole2: $y, subSubRole3: $x); };";
        String specialisedRelation3 = "{ (subRole1: $u, subRole2: $v, subSubRole3: $q); };";
        String specialisedRelation4 = "{ (subRole1: $y, subRole2: $y, subSubRole3: $x); };";
        String specialisedRelation5 = "{ (subRole1: $u, subRole1: $v, subSubRole3: $q); };";

        unification(parentRelation, specialisedRelation, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation2, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation3, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation4, UnifierType.RULE,false, false, tx);
        nonExistentUnifier(parentRelation, specialisedRelation5);
    }

    @Test
    public void testUnification_VariousResourceAtoms(){
        String resource = "{ $x has resource $r;$r 'f'; };";
        String resource2 = "{ $r has resource $x;$x 'f'; };";
        String resource3 = "{ $r has resource 'f'; };";
        String resource4 = "{ $x has resource $y via $r;$y 'f'; };";
        String resource5 = "{ $y has resource $r via $x;$r 'f'; };";
        unification(resource, resource2, UnifierType.RULE,true, true, tx);
        unification(resource, resource3, UnifierType.RULE,true, true, tx);
        unification(resource2, resource3, UnifierType.RULE,true, true, tx);
        unification(resource4, resource5, UnifierType.RULE,true, true, tx);

        unification(resource, resource2, UnifierType.EXACT,true, true, tx);
        unification(resource, resource3, UnifierType.EXACT,true, true, tx);
        unification(resource2, resource3, UnifierType.EXACT,true, true, tx);
        unification(resource4, resource5, UnifierType.EXACT,true, true, tx);
    }

    @Test
    public void testUnification_VariousTypeAtoms(){
        String type = "{ $x isa baseRoleEntity; };";
        String type2 = "{ $y isa baseRoleEntity; };";
        String userDefinedType = "{ $y isa $x;$x type baseRoleEntity; };";
        String userDefinedType2 = "{ $u isa $v;$v type baseRoleEntity; };";

        unification(type, type2, UnifierType.EXACT, true, true, tx);
        unification(userDefinedType, userDefinedType2, UnifierType.EXACT, true, true, tx);

        unification(type, type2, UnifierType.RULE, true, true, tx);
        unification(userDefinedType, userDefinedType2, UnifierType.RULE, true, true, tx);
        //TODO user defined-generated test
        //unification(type, userDefinedType, true, true, tx);
    }

    @Test
    public void testUnification_ParentHasFewerRelationPlayers() {
        String childString = "{ (subRole1: $y, subRole2: $x) isa binary; };";
        String parentString = "{ (subRole1: $x) isa binary; };";
        String parentString2 = "{ (subRole2: $y) isa binary; };";

        ReasonerAtomicQuery childQuery = reasonerQueryFactory.atomic(conjunction(childString));
        ReasonerAtomicQuery parentQuery = reasonerQueryFactory.atomic(conjunction(parentString));
        ReasonerAtomicQuery parentQuery2 = reasonerQueryFactory.atomic(conjunction(parentString2));

        Atom childAtom = childQuery.getAtom();
        Atom parentAtom = parentQuery.getAtom();
        Atom parentAtom2 = parentQuery2.getAtom();

        List<ConceptMap> childAnswers = tx.execute(childQuery.getQuery(), false);
        List<ConceptMap> parentAnswers = tx.execute(parentQuery.getQuery(), false);
        List<ConceptMap> parentAnswers2 = tx.execute(parentQuery2.getQuery(), false);

        Unifier unifier = childAtom.getUnifier(parentAtom, UnifierType.EXACT);
        Unifier unifier2 = childAtom.getUnifier(parentAtom2, UnifierType.EXACT);

        assertCollectionsNonTriviallyEqual(
                parentAnswers,
                childAnswers.stream()
                        .map(unifier::apply)
                        .map(a -> a.project(parentQuery.getVarNames()))
                        .distinct()
                        .collect(Collectors.toList())
        );
        assertCollectionsNonTriviallyEqual(
                parentAnswers2,
                childAnswers.stream()
                        .map(unifier2::apply)
                        .map(a -> a.project(parentQuery2.getVarNames()))
                        .distinct()
                        .collect(Collectors.toList())
        );
    }

    @Test
    public void testUnification_ResourceWithIndirectValuePredicate(){
        String resource = "{ $x has resource $r;$r == 'f'; };";
        String resource2 = "{ $r has resource $x;$x == 'f'; };";
        String resource3 = "{ $r has resource 'f'; };";

        ReasonerAtomicQuery resourceQuery = reasonerQueryFactory.atomic(conjunction(resource));
        ReasonerAtomicQuery resourceQuery2 = reasonerQueryFactory.atomic(conjunction(resource2));
        ReasonerAtomicQuery resourceQuery3 = reasonerQueryFactory.atomic(conjunction(resource3));

        String type = "{ $x isa resource;$x id " + tx.execute(resourceQuery.getQuery(), false).iterator().next().get("r").id().getValue()  + "; };";
        ReasonerAtomicQuery typeQuery = reasonerQueryFactory.atomic(conjunction(type));
        Atom typeAtom = typeQuery.getAtom();

        Atom resourceAtom = resourceQuery.getAtom();
        Atom resourceAtom2 = resourceQuery2.getAtom();
        Atom resourceAtom3 = resourceQuery3.getAtom();

        Unifier unifier = resourceAtom.getUnifier(typeAtom, UnifierType.RULE);
        Unifier unifier2 = resourceAtom2.getUnifier(typeAtom, UnifierType.RULE);
        Unifier unifier3 = resourceAtom3.getUnifier(typeAtom, UnifierType.RULE);

        ConceptMap typeAnswer = tx.execute(typeQuery.getQuery(), false).iterator().next();
        ConceptMap resourceAnswer = tx.execute(resourceQuery.getQuery(), false).iterator().next();
        ConceptMap resourceAnswer2 = tx.execute(resourceQuery2.getQuery(), false).iterator().next();
        ConceptMap resourceAnswer3 = tx.execute(resourceQuery3.getQuery(), false).iterator().next();

        assertEquals(typeAnswer.get("x"), unifier.apply(resourceAnswer).get("x"));
        assertEquals(typeAnswer.get("x"), unifier2.apply(resourceAnswer2).get("x"));
        assertEquals(typeAnswer.get("x"), unifier3.apply(resourceAnswer3).get("x"));
    }

    @Test
    public void testRewriteAndUnification(){
        String parentString = "{ $r (subRole1: $x) isa binary; };";
        Atom parentAtom = reasonerQueryFactory.atomic(conjunction(parentString)).getAtom();
        Variable parentVarName = parentAtom.getVarName();

        String childPatternString = "(subRole1: $x, subRole2: $y) isa binary;";
        InferenceRule testRule = new InferenceRule(
                tx.putRule("Checking Rewrite & Unification",
                           Graql.parsePattern(childPatternString),
                           Graql.parsePattern(childPatternString)),
                reasonerQueryFactory)
                .rewrite(parentAtom);

        RelationAtom headAtom = (RelationAtom) testRule.getHead().getAtom();
        Variable headVarName = headAtom.getVarName();

        Unifier unifier = Iterables.getOnlyElement(testRule.getMultiUnifier(parentAtom));
        Unifier correctUnifier = new UnifierImpl(
                ImmutableMap.of(
                        new Variable("x"), new Variable("x"),
                        headVarName, parentVarName)
        );

        assertTrue(unifier.containsAll(correctUnifier));

        Multimap<Role, Variable> roleMap = roleSetMap(headAtom.getRoleVarMap());
        Collection<Variable> wifeEntry = roleMap.get(tx.getRole("subRole1"));
        assertEquals(wifeEntry.size(), 1);
        assertEquals(wifeEntry.iterator().next(), new Variable("x"));
    }

    @Test
    public void testUnification_MatchAllParentAtom(){
        String parentString = "{ $r($a, $x); };";
        String childString = "{ $rel (baseRole1: $z, baseRole2: $b) isa binary; };";
        Atom parent = reasonerQueryFactory.atomic(conjunction(parentString)).getAtom();
        Atom child = reasonerQueryFactory.atomic(conjunction(childString)).getAtom();

        MultiUnifier multiUnifier = child.getMultiUnifier(parent, UnifierType.RULE);
        Unifier correctUnifier = new UnifierImpl(
                ImmutableMap.of(
                        new Variable("z"), new Variable("a"),
                        new Variable("b"), new Variable("x"),
                        child.getVarName(), parent.getVarName())
        );
        Unifier correctUnifier2 = new UnifierImpl(
                ImmutableMap.of(
                        new Variable("z"), new Variable("x"),
                        new Variable("b"), new Variable("a"),
                        child.getVarName(), parent.getVarName())
        );
        assertEquals(multiUnifier.size(), 2);
        multiUnifier.forEach(u -> assertTrue(u.containsAll(correctUnifier) || u.containsAll(correctUnifier2)));
    }

    @Test
    public void testUnification_IndirectRoles(){
        Statement basePattern = var()
                .rel(var("baseRole1").type("subRole1"), var("y1"))
                .rel(var("baseRole2").type("subSubRole2"), var("y2"))
                .isa("binary");

        ReasonerAtomicQuery baseQuery = reasonerQueryFactory.atomic(Graql.and(Sets.newHashSet(basePattern)));
        ReasonerAtomicQuery childQuery = reasonerQueryFactory
                .atomic(conjunction(
                        "{($r1: $x1, $r2: $x2) isa binary;" +
                                "$r1 type subRole1;" +
                                "$r2 type subSubRole2; };"
                        ));
        ReasonerAtomicQuery parentQuery = reasonerQueryFactory
                .atomic(conjunction(
                        "{ ($R1: $x, $R2: $y) isa binary;" +
                                "$R1 type subRole1;" +
                                "$R2 type subSubRole2; };"
                        ));
        unification(parentQuery, childQuery, UnifierType.EXACT, true, true);
        unification(baseQuery, parentQuery, UnifierType.EXACT, true, true);
        unification(baseQuery, childQuery, UnifierType.EXACT, true, true);
    }

    @Test
    public void testUnification_IndirectRoles_NoRelationType(){
        Statement basePattern = var()
                .rel(var("baseRole1").type("subRole1"), var("y1"))
                .rel(var("baseRole2").type("subSubRole2"), var("y2"));

        ReasonerAtomicQuery baseQuery = reasonerQueryFactory.atomic(Graql.and(Sets.newHashSet(basePattern)));
        ReasonerAtomicQuery childQuery = reasonerQueryFactory
                .atomic(conjunction(
                        "{ ($r1: $x1, $r2: $x2); " +
                                "$r1 type subRole1;" +
                                "$r2 type subSubRole2; };"
                        ));
        ReasonerAtomicQuery parentQuery = reasonerQueryFactory
                .atomic(conjunction(
                        "{ ($R1: $x, $R2: $y); " +
                                "$R1 type subRole1;" +
                                "$R2 type subSubRole2; };"
                        ));
        unification(parentQuery, childQuery, UnifierType.EXACT, true, true);
        unification(baseQuery, parentQuery, UnifierType.EXACT, true, true);
        unification(baseQuery, childQuery, UnifierType.EXACT, true, true);
    }

    private void roleInference(String patternString, ImmutableSetMultimap<Role, Variable> expectedRoleMAp, Transaction tx){
        RelationAtom atom = (RelationAtom) reasonerQueryFactory.atomic(conjunction(patternString)).getAtom();
        Multimap<Role, Variable> roleMap = roleSetMap(atom.getRoleVarMap());
        assertEquals(expectedRoleMAp, roleMap);

    }

    /**
     * checks that the child query is not unifiable with parent - a unifier does not exist
     * @param parentQuery parent query
     * @param childQuery child query
     */
    private void nonExistentUnifier(ReasonerAtomicQuery parentQuery, ReasonerAtomicQuery childQuery){
        Atom childAtom = childQuery.getAtom();
        Atom parentAtom = parentQuery.getAtom();
        assertTrue(childAtom.getMultiUnifier(parentAtom, UnifierType.EXACT).isEmpty());
    }

    private void nonExistentUnifier(String parentPatternString, String childPatternString){
        nonExistentUnifier(
                reasonerQueryFactory.atomic(conjunction(parentPatternString)),
                reasonerQueryFactory.atomic(conjunction(childPatternString))
        );
    }

    /**
     * checks the correctness and uniqueness of an exact unifier required to unify child query with parent
     * @param parentQuery parent query
     * @param childQuery child query
     * @param checkInverse flag specifying whether the inverse equality u^{-1}=u(parent, child) of the unifier u(child, parent) should be checked
     * @param checkEquality if true the parent and child answers will be checked for equality, otherwise they are checked for containment of child answers in parent
     */
    private void unification(ReasonerAtomicQuery parentQuery, ReasonerAtomicQuery childQuery, 
                             UnifierType unifierType, boolean checkInverse, boolean checkEquality){
        Atom childAtom = childQuery.getAtom();
        Atom parentAtom = parentQuery.getAtom();

        Unifier unifier = childAtom.getMultiUnifier(parentAtom, unifierType).getUnifier();

        List<ConceptMap> childAnswers = tx.execute(childQuery.getQuery(), false);
        List<ConceptMap> unifiedAnswers = childAnswers.stream()
                .map(unifier::apply)
                .filter(a -> !a.isEmpty())
                .collect(Collectors.toList());
        List<ConceptMap> parentAnswers = tx.execute(parentQuery.getQuery(), false);

        if (checkInverse) {
            Unifier unifier2 = parentAtom.getUnifier(childAtom, unifierType);
            assertEquals(unifier.inverse(), unifier2);
            assertEquals(unifier, unifier2.inverse());
        }

        assertTrue(!childAnswers.isEmpty());
        assertTrue(!unifiedAnswers.isEmpty());
        assertTrue(!parentAnswers.isEmpty());

        if (!checkEquality){
            assertTrue(parentAnswers.containsAll(unifiedAnswers));
        } else {
            assertCollectionsNonTriviallyEqual(parentAnswers, unifiedAnswers);
            Unifier inverse = unifier.inverse();
            List<ConceptMap> parentToChild = parentAnswers.stream().map(inverse::apply).collect(Collectors.toList());
            assertCollectionsNonTriviallyEqual(parentToChild, childAnswers);
        }
    }

    private void unification(String parentPatternString, String childPatternString, 
                             UnifierType unifierType, boolean checkInverse, boolean checkEquality, Transaction tx){
        unification(
                reasonerQueryFactory.atomic(conjunction(parentPatternString)),
                reasonerQueryFactory.atomic(conjunction(childPatternString)),
                unifierType,
                checkInverse,
                checkEquality);
    }

    private Multimap<Role, Variable> roleSetMap(Multimap<Role, Variable> roleVarMap) {
        Multimap<Role, Variable> roleMap = HashMultimap.create();
        roleVarMap.entries().forEach(e -> roleMap.put(e.getKey(), e.getValue()));
        return roleMap;
    }

    private Conjunction<Statement> conjunction(String patternString){
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}
