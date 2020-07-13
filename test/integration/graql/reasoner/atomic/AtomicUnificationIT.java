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
import grakn.core.test.rule.GraknTestStorage;
import grakn.core.test.rule.SessionUtil;
import grakn.core.test.rule.TestTransactionProvider;
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

import static grakn.core.test.common.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static graql.lang.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class AtomicUnificationIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session session;

    private Transaction tx;
    // factories exposed by a test transaction, bound to the lifetime of a tx
    private ReasonerQueryFactory reasonerQueryFactory;

    @BeforeClass
    public static void loadContext(){
        Config mockServerConfig = storage.createCompatibleServerConfig();
        session = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);

        // define schema
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.parse("define " +
                    "organisation sub entity," +
                    "  has name, " +
                    "  plays employer," +
                    "  plays employee, " + // awkward
                    "  plays employee-recommender; " +
                    "part-time-organisation sub organisation, " +
                    "  plays part-time-employer, " +
                    "  plays part-time-employee, " +
                    "  plays part-time-employee-recommender;" +
                    "part-time-driving-hire sub part-time-organisation, " +
                    "  plays part-time-taxi, " +
                    "  plays part-time-driver-recommender, " +
                    "  plays night-shift-driver, " + // awkward
                    "  plays day-shift-driver; " + // awkward

                    // define complex role hierarchy that can be easily converted into a relation AND role hierarchy in future
                    "employment sub relation, " +
                    "  relates employer," +
                    "  relates employee," +
                    "  relates employee-recommender," +
                    "  relates part-time-employer," + // could come from 'part-time-employment' sub 'employment'
                    "  relates part-time-employee," +
                    "  relates part-time-employee-recommender," +
                    "  relates night-shift-driver," + // could come from 'part-time-driving' sub 'part-time-employment'
                    "  relates day-shift-driver," + // could come from 'part-time-driving' sub 'part-time-employment'
                    "  relates part-time-taxi," +
                    "  relates part-time-driver-recommender;" +

                    // the explicit hierarchy will eventually become overriden roles in sub-relations
                    "part-time-employer sub employer;" +
                    "part-time-employee sub employee;" +
                    "part-time-employee-recommender sub employee-recommender;" +
                    "part-time-taxi sub part-time-employer;" +
                    "night-shift-driver sub part-time-employee;" +
                    "day-shift-driver sub part-time-employee;" +
                    "part-time-driver-recommender sub part-time-employee-recommender;" +
                    "name sub attribute, value string;").asDefine());



            // insert:
            tx.execute(Graql.parse("insert " +
                    "(part-time-taxi: $x, night-shift-driver: $y) isa employment; " +
                    "(part-time-employer: $x, part-time-employee: $y, part-time-driver-recommender: $z) isa employment; " +
                    // note duplicate RP, needed to satisfy one of the child queries
                    "(part-time-taxi: $x, part-time-employee: $x, part-time-driver-recommender: $z) isa employment; " +
                    "$x isa part-time-driving-hire;" +
                    "$y isa part-time-driving-hire;" +
                    "$z isa part-time-driving-hire;").asInsert());
            tx.commit();
        }

        // insert schema and data for first set of tests to do with ownership
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.parse("define " +
                    "car sub entity, " +
                    "  plays owned," +
                    "  has name; " +
                    "ownership sub relation," +
                    "  relates owner, " +
                    "  relates owned; " +
                    "company sub organisation, " +
                    "  plays owner;").asDefine());

            tx.execute(Graql.parse("insert " +
                    "$x isa company, has name \"Google\"; " +
                    "$y isa car, has name \"Betty\"; " +
                    "(owner: $x, owned: $y) isa ownership;").asInsert());
            tx.commit();
        }
    }

    @AfterClass
    public static void closeSession(){
        session.close();
    }

    @Before
    public void setUp(){
        tx = session.transaction(Transaction.Type.WRITE);
        TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction) tx);
        reasonerQueryFactory = testTx.reasonerQueryFactory();
    }

    @After
    public void tearDown(){
        tx.close();
    }

    @Test
    public void testUnification_RelationWithRolesExchanged(){
        String relation = "{ (owner: $x, owned: $y) isa ownership; };";
        String relation2 = "{ (owner: $y, owned: $x) isa ownership; };";
        unification(relation, relation2, UnifierType.EXACT,true, true, tx);
    }

    @Test
    public void testUnification_RelationWithMetaRole(){
        String relation = "{ (owner: $x, role: $y) isa ownership; };";
        String relation2 = "{ (owner: $y, role: $x) isa ownership; };";
        unification(relation, relation2, UnifierType.EXACT,true, true, tx);
    }

    @Test
    public void testUnification_RelationWithRelationVar(){
        String relation = "{ $x (owner: $r, owned: $z) isa ownership; };";
        String relation2 = "{ $r (owner: $x, owned: $y) isa ownership; };";
        unification(relation, relation2,UnifierType.EXACT, true, true, tx);
    }

    @Test
    public void testUnification_RelationWithMetaRolesAndIds(){
        Concept instance = tx.execute(Graql.parse("match $x isa company; get;").asGet()).iterator().next().get("x");
        String relation = "{ (role: $x, role: $y) isa ownership; $y id " + instance.id().getValue() + "; };";
        String relation2 = "{ (role: $z, role: $v) isa ownership; $z id " + instance.id().getValue() + "; };";
        String relation3 = "{ (role: $z, role: $v) isa ownership; $v id " + instance.id().getValue() + "; };";

        unification(relation, relation2, UnifierType.EXACT,true, true, tx);
        unification(relation, relation3, UnifierType.EXACT, true, true, tx);
        unification(relation2, relation3, UnifierType.EXACT,true, true, tx);
    }

    @Test
    public void testUnification_BinaryRelationWithRoleHierarchy_ParentWithBaseRoles(){
        String parentRelation = "{ (employer: $x, employee: $y); };";
        String specialisedRelation = "{ (part-time-employer: $u, part-time-employee: $v); };";
        String specialisedRelation2 = "{ (part-time-employer: $y, part-time-employee: $x); };";
        String specialisedRelation3 = "{ (part-time-taxi: $u, night-shift-driver: $v); };";
        String specialisedRelation4 = "{ (part-time-taxi: $y, night-shift-driver: $x); };";
        // both roles derive from one parent role, fails
        String specialisedRelation5 = "{ (part-time-employee: $u, night-shift-driver: $v); };";

        unification(parentRelation, specialisedRelation, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation2, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation3, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation4, UnifierType.RULE,false, false, tx);
        nonExistentUnifier(parentRelation, specialisedRelation5);
    }

    @Test
    public void testUnification_BinaryRelationWithRoleHierarchy_ParentWithSubRoles(){
        String parentRelation = "{ (part-time-employer: $x, part-time-employee: $y); };";
        String specialisedRelation = "{ (part-time-employer: $u, night-shift-driver: $v); };";
        String specialisedRelation2 = "{ (part-time-employer: $y, night-shift-driver: $x); };";
        String specialisedRelation3 = "{ (part-time-taxi: $u, night-shift-driver: $v); };";
        String specialisedRelation4 = "{ (part-time-taxi: $y, night-shift-driver: $x); };";
        String specialisedRelation5 = "{ (part-time-taxi: $u, employee-recommender: $v); };";
        String specialisedRelation6 = "{ (employer: $u, employee: $v); };";

        unification(parentRelation, specialisedRelation, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation2, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation3, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation4, UnifierType.RULE,false, false, tx);
        nonExistentUnifier(parentRelation, specialisedRelation5);
        nonExistentUnifier(parentRelation, specialisedRelation6);
    }

    @Test
    public void testUnification_TernaryRelationWithRoleHierarchy_ParentWithBaseRoles(){
        String parentRelation = "{ (employer: $x, employee: $y, employee-recommender: $z); };";
        String specialisedRelation = "{ (employer: $u, part-time-employee: $v, part-time-driver-recommender: $q); };";
        String specialisedRelation2 = "{ (employer: $z, part-time-employee: $y, part-time-driver-recommender: $x); };";
        String specialisedRelation3 = "{ (part-time-employer: $u, part-time-employee: $v, part-time-driver-recommender: $q); };";
        String specialisedRelation4 = "{ (part-time-employer: $y, part-time-employee: $z, part-time-driver-recommender: $x); };";
        String specialisedRelation5 = "{ (part-time-employer: $u, part-time-employer: $v, part-time-driver-recommender: $q); };";

        unification(parentRelation, specialisedRelation, UnifierType.RULE,false, true, tx);
        unification(parentRelation, specialisedRelation2, UnifierType.RULE,false, true, tx);
        unification(parentRelation, specialisedRelation3, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation4, UnifierType.RULE,false, false, tx);
        nonExistentUnifier(parentRelation, specialisedRelation5);
    }

    @Test
    public void testUnification_TernaryRelationWithRoleHierarchy_ParentWithSubRoles(){
        String parentRelation = "{ (part-time-employer: $x, part-time-employee: $y, part-time-employee-recommender: $z); };";
        String specialisedRelation = "{ (employer: $u, part-time-employee: $v, part-time-driver-recommender: $q); };";
        String specialisedRelation2 = "{ (part-time-employer: $u, part-time-employee: $v, part-time-driver-recommender: $q); };";
        String specialisedRelation3 = "{ (part-time-employer: $y, part-time-employee: $z, part-time-driver-recommender: $x); };";
        String specialisedRelation4 = "{ (part-time-taxi: $u, part-time-employee: $v, part-time-driver-recommender: $q); };";
        String specialisedRelation5 = "{ (part-time-taxi: $y, part-time-employee: $z, part-time-driver-recommender: $x); };";
        String specialisedRelation6 = "{ (part-time-employer: $u, part-time-employer: $v, part-time-driver-recommender: $q); };";

        nonExistentUnifier(parentRelation, specialisedRelation);
        unification(parentRelation, specialisedRelation2, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation3, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation4, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation5, UnifierType.RULE,false, false, tx);
        nonExistentUnifier(parentRelation, specialisedRelation6);
    }

    @Test
    public void testUnification_TernaryRelationWithRoleHierarchy_ParentWithBaseRoles_childrenRepeatRolePlayers(){
        String parentRelation = "{ (employer: $x, employee: $y, employee-recommender: $z); };";
        String specialisedRelation = "{ (employer: $u, part-time-employee: $u, part-time-driver-recommender: $q); };";
        String specialisedRelation2 = "{ (employer: $y, part-time-employee: $y, part-time-driver-recommender: $x); };";
        String specialisedRelation3 = "{ (part-time-employer: $u, part-time-employee: $u, part-time-driver-recommender: $q); };";
        String specialisedRelation4 = "{ (part-time-employer: $y, part-time-employee: $y, part-time-driver-recommender: $x); };";
        String specialisedRelation5 = "{ (part-time-employer: $u, part-time-employer: $u, part-time-driver-recommender: $q); };";

        unification(parentRelation, specialisedRelation, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation2, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation3, UnifierType.RULE, false, false, tx);
        unification(parentRelation, specialisedRelation4, UnifierType.RULE,false, false, tx);
        nonExistentUnifier(parentRelation, specialisedRelation5);
    }

    @Test
    public void testUnification_TernaryRelationWithRoleHierarchy_ParentWithBaseRoles_parentRepeatRolePlayers(){
        String parentRelation = "{ (employer: $x, employee: $x, employee-recommender: $y); };";
        String specialisedRelation = "{ (employer: $u, part-time-employee: $v, part-time-driver-recommender: $q); };";
        String specialisedRelation2 = "{ (employer: $z, part-time-employee: $y, part-time-driver-recommender: $x); };";
        String specialisedRelation3 = "{ (part-time-employer: $u, part-time-employee: $v, part-time-driver-recommender: $q); };";
        String specialisedRelation4 = "{ (part-time-employer: $y, part-time-employee: $y, part-time-driver-recommender: $x); };";
        String specialisedRelation5 = "{ (part-time-employer: $u, part-time-employer: $v, part-time-driver-recommender: $q); };";

        unification(parentRelation, specialisedRelation, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation2, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation3, UnifierType.RULE,false, false, tx);
        unification(parentRelation, specialisedRelation4, UnifierType.RULE,false, false, tx);
        nonExistentUnifier(parentRelation, specialisedRelation5);
    }

    @Test
    public void testUnification_VariousAttributeAtoms(){
        String attribute = "{ $x has name $r;$r 'Google'; };";
        String attribute2 = "{ $r has name $x;$x 'Google'; };";
        String attribute3 = "{ $r has name 'Google'; };";
        unification(attribute, attribute2, UnifierType.RULE,true, true, tx);
        unification(attribute, attribute3, UnifierType.RULE,true, true, tx);
        unification(attribute2, attribute3, UnifierType.RULE,true, true, tx);

        unification(attribute, attribute2, UnifierType.EXACT,true, true, tx);
        unification(attribute, attribute3, UnifierType.EXACT,true, true, tx);
        unification(attribute2, attribute3, UnifierType.EXACT,true, true, tx);
    }

    @Test
    public void testUnification_VariousTypeAtoms(){
        String type = "{ $x isa company; };";
        String type2 = "{ $y isa company; };";
        String userDefinedType = "{ $y isa $x;$x type company; };";
        String userDefinedType2 = "{ $u isa $v;$v type company; };";

        unification(type, type2, UnifierType.EXACT, true, true, tx);
        unification(userDefinedType, userDefinedType2, UnifierType.EXACT, true, true, tx);

        unification(type, type2, UnifierType.RULE, true, true, tx);
        unification(userDefinedType, userDefinedType2, UnifierType.RULE, true, true, tx);
        //TODO user defined-generated test
        //unification(type, userDefinedType, true, true, tx);
    }

    @Test
    public void testUnification_ParentHasFewerRelationPlayers() {
        String childString = "{ (part-time-employer: $y, part-time-employee: $x) isa employment; };";
        String parentString = "{ (part-time-employer: $x) isa employment; };";
        String parentString2 = "{ (part-time-employee: $y) isa employment; };";

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
    public void testUnification_AttributeWithIndirectValuePredicate(){
        String attribute = "{ $x has name $r; $r == 'Google'; };";
        String attribute2 = "{ $r has name $x; $x == 'Google'; };";
        String attribute3 = "{ $r has name 'Google'; };";

        ReasonerAtomicQuery attributeQuery = reasonerQueryFactory.atomic(conjunction(attribute));
        ReasonerAtomicQuery attributeQuery2 = reasonerQueryFactory.atomic(conjunction(attribute2));
        ReasonerAtomicQuery attributeQuery3 = reasonerQueryFactory.atomic(conjunction(attribute3));

        String type = "{ $x isa name;$x id " + tx.execute(attributeQuery.getQuery(), false).iterator().next().get("r").id().getValue()  + "; };";
        ReasonerAtomicQuery typeQuery = reasonerQueryFactory.atomic(conjunction(type));
        Atom typeAtom = typeQuery.getAtom();

        Atom attributeAtom = attributeQuery.getAtom();
        Atom attributeAtom2 = attributeQuery2.getAtom();
        Atom attributeAtom3 = attributeQuery3.getAtom();

        Unifier unifier = attributeAtom.getUnifier(typeAtom, UnifierType.RULE);
        Unifier unifier2 = attributeAtom2.getUnifier(typeAtom, UnifierType.RULE);
        Unifier unifier3 = attributeAtom3.getUnifier(typeAtom, UnifierType.RULE);

        ConceptMap typeAnswer = tx.execute(typeQuery.getQuery(), false).iterator().next();
        ConceptMap attributeAnswer = tx.execute(attributeQuery.getQuery(), false).iterator().next();
        ConceptMap attributeAnswer2 = tx.execute(attributeQuery2.getQuery(), false).iterator().next();
        ConceptMap attributeAnswer3 = tx.execute(attributeQuery3.getQuery(), false).iterator().next();

        assertEquals(typeAnswer.get("x"), unifier.apply(attributeAnswer).get("x"));
        assertEquals(typeAnswer.get("x"), unifier2.apply(attributeAnswer2).get("x"));
        assertEquals(typeAnswer.get("x"), unifier3.apply(attributeAnswer3).get("x"));
    }

    @Test
    public void testRewriteAndUnification(){
        String parentString = "{ $r (part-time-employer: $x) isa employment; };";
        Atom parentAtom = reasonerQueryFactory.atomic(conjunction(parentString)).getAtom();
        Variable parentVarName = parentAtom.getVarName();

        String childPatternString = "(part-time-employer: $x, part-time-employee: $y) isa employment;";
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
        Collection<Variable> wifeEntry = roleMap.get(tx.getRole("part-time-employer"));
        assertEquals(wifeEntry.size(), 1);
        assertEquals(wifeEntry.iterator().next(), new Variable("x"));
    }

    @Test
    public void testUnification_MatchAllParentAtom(){
        String parentString = "{ $r($a, $x); };";
        String childString = "{ $rel (employer: $z, employee: $b) isa employment; };";
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
                .rel(var("employer").type("part-time-employer"), var("y1"))
                .rel(var("employee").type("night-shift-driver"), var("y2"))
                .isa("employment");

        ReasonerAtomicQuery baseQuery = reasonerQueryFactory.atomic(Graql.and(Sets.newHashSet(basePattern)));
        ReasonerAtomicQuery childQuery = reasonerQueryFactory
                .atomic(conjunction(
                        "{($r1: $x1, $r2: $x2) isa employment;" +
                                "$r1 type part-time-employer;" +
                                "$r2 type night-shift-driver; };"
                        ));
        ReasonerAtomicQuery parentQuery = reasonerQueryFactory
                .atomic(conjunction(
                        "{ ($R1: $x, $R2: $y) isa employment;" +
                                "$R1 type part-time-employer;" +
                                "$R2 type night-shift-driver; };"
                        ));
        unification(parentQuery, childQuery, UnifierType.EXACT, true, true);
        unification(baseQuery, parentQuery, UnifierType.EXACT, true, true);
        unification(baseQuery, childQuery, UnifierType.EXACT, true, true);
    }

    @Test
    public void testUnification_IndirectRoles_NoRelationType(){
        Statement basePattern = var()
                .rel(var("employer").type("part-time-employer"), var("y1"))
                .rel(var("employee").type("night-shift-driver"), var("y2"));

        ReasonerAtomicQuery baseQuery = reasonerQueryFactory.atomic(Graql.and(Sets.newHashSet(basePattern)));
        ReasonerAtomicQuery childQuery = reasonerQueryFactory
                .atomic(conjunction(
                        "{ ($r1: $x1, $r2: $x2); " +
                                "$r1 type part-time-employer;" +
                                "$r2 type night-shift-driver; };"
                        ));
        ReasonerAtomicQuery parentQuery = reasonerQueryFactory
                .atomic(conjunction(
                        "{ ($R1: $x, $R2: $y); " +
                                "$R1 type part-time-employer;" +
                                "$R2 type night-shift-driver; };"
                        ));
        unification(parentQuery, childQuery, UnifierType.EXACT, true, true);
        unification(baseQuery, parentQuery, UnifierType.EXACT, true, true);
        unification(baseQuery, childQuery, UnifierType.EXACT, true, true);
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
