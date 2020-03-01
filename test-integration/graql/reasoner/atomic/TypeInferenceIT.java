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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import grakn.core.common.config.Config;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import grakn.core.util.GraqlTestUtil;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.util.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class TypeInferenceIT {

    private static String resourcePath = "test-integration/graql/reasoner/resources/";

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session testContextSession;
    private Transaction tx;

    @BeforeClass
    public static void loadContext(){
        Config mockServerConfig = storage.createCompatibleServerConfig();
        testContextSession = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        loadFromFileAndCommit(resourcePath,"typeInferenceTest.gql", testContextSession);
    }

    @AfterClass
    public static void closeSession(){
        testContextSession.close();
    }


    @Before
    public void setup() {
        tx = testContextSession.writeTransaction();
    }

    @After
    public void tearDown() {
        tx.close();
    }

    @Test
    public void whenCalculatingTypeMaps_typesAreCollapsedCorrectly(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        ReasonerQuery query = reasonerQueryFactory.atomic(conjunction("{($x); $x isa singleRoleEntity;$x isa twoRoleEntity;};"));
        assertEquals(tx.getEntityType("twoRoleEntity"), Iterables.getOnlyElement(query.getVarTypeMap().get(new Variable("x"))));

        query = reasonerQueryFactory.atomic(conjunction("{($x); $x isa entity;$x isa singleRoleEntity;};"));
        assertEquals(tx.getType(Label.of("singleRoleEntity")), Iterables.getOnlyElement(query.getVarTypeMap().get(new Variable("x"))));
    }

    @Test
    public void testTypeInference_singleGuard() {
        //parent of all roles so all relations possible
        String patternString = "{ $x isa noRoleEntity; ($x, $y); };";
        String subbedPatternString = "{ $x id " + conceptId(tx, "noRoleEntity") + ";($x, $y); };";

        //SRE -> rel2
        //sub(SRE)=TRE -> rel3
        String patternString2 = "{ $x isa singleRoleEntity; ($x, $y); };";
        String subbedPatternString2 = "{ $x id " + conceptId(tx, "singleRoleEntity") + ";($x, $y); };";

        //TRE -> rel3
        String patternString3 = "{ $x isa twoRoleEntity; ($x, $y); };";
        String subbedPatternString3 = "{ $x id " + conceptId(tx, "twoRoleEntity") + ";($x, $y); };";

        List<Type> possibleTypes = Lists.newArrayList(
                tx.getType(Label.of("anotherTwoRoleBinary")),
                tx.getType(Label.of("threeRoleBinary"))
        );

        typeInference(allRelations(tx), patternString, subbedPatternString, (TestTransactionProvider.TestTransaction)tx);
        typeInference(possibleTypes, patternString2, subbedPatternString2, (TestTransactionProvider.TestTransaction)tx);
        typeInference(possibleTypes, patternString3, subbedPatternString3, (TestTransactionProvider.TestTransaction)tx);
    }

    @Test
    public void testTypeInference_doubleGuard() {
        //{rel2, rel3} ^ {rel1, rel2, rel3} = {rel2, rel3}
        String patternString = "{ $x isa singleRoleEntity; ($x, $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString = "{($x, $y);" +
                "$x id " + conceptId(tx, "singleRoleEntity") + ";" +
                "$y id " + conceptId(tx, "anotherTwoRoleEntity") +";};";

        //{rel2, rel3} ^ {rel1, rel2, rel3} = {rel2, rel3}
        String patternString2 = "{ $x isa twoRoleEntity; ($x, $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString2 = "{($x, $y);" +
                "$x id " + conceptId(tx, "twoRoleEntity") + ";" +
                "$y id " + conceptId(tx, "anotherTwoRoleEntity") +";};";

        //{rel1} ^ {rel1, rel2, rel3} = {rel1}
        String patternString3 = "{ $x isa yetAnotherSingleRoleEntity; ($x, $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString3 = "{($x, $y);" +
                "$x id " + conceptId(tx, "yetAnotherSingleRoleEntity") + ";" +
                "$y id " + conceptId(tx, "anotherTwoRoleEntity") +";};";

        List<Type> possibleTypes = Lists.newArrayList(
                tx.getType(Label.of("anotherTwoRoleBinary")),
                tx.getType(Label.of("threeRoleBinary"))
        );

        List<Type> possibleTypes2 = Collections.singletonList(tx.getType(Label.of("twoRoleBinary")));

        typeInference(possibleTypes, patternString, subbedPatternString, (TestTransactionProvider.TestTransaction)tx);
        typeInference(possibleTypes, patternString2, subbedPatternString2, (TestTransactionProvider.TestTransaction)tx);
        typeInference(possibleTypes2, patternString3, subbedPatternString3, (TestTransactionProvider.TestTransaction)tx);
    }

    @Test
    public void testTypeInference_singleRole() {
        String patternString = "{ (role1: $x, $y); };";
        String patternString2 = "{ (role2: $x, $y); };";
        String patternString3 = "{ (role3: $x, $y); };";

        List<Type> possibleTypes = Collections.singletonList(tx.getSchemaConcept(Label.of("twoRoleBinary")));
        List<Type> possibleTypes2 = Lists.newArrayList(
                tx.getType(Label.of("anotherTwoRoleBinary")),
                tx.getType(Label.of("threeRoleBinary"))
        );

        typeInference(possibleTypes, patternString, tx);
        typeInference(allRelations(tx), patternString2, tx);
        typeInference(possibleTypes2, patternString3, tx);
    }

    @Test
    public void testTypeInference_singleRole_subType() {
        String patternString = "{ (subRole2: $x, $y); };";
        typeInference(Collections.singletonList(tx.getSchemaConcept(Label.of("threeRoleBinary"))), patternString, tx);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard() {

        //{rel1, rel2, rel3} ^ {rel2, rel3}
        String patternString = "{ (role2: $x, $y); $y isa singleRoleEntity; };";
        String subbedPatternString = "{(role2: $x, $y);" +
                "$y id " + conceptId(tx, "singleRoleEntity") + ";};";
        //{rel1, rel2, rel3} ^ {rel2, rel3}
        String patternString2 = "{ (role2: $x, $y); $y isa twoRoleEntity; };";
        String subbedPatternString2 = "{(role2: $x, $y);" +
                "$y id " + conceptId(tx, "twoRoleEntity") + ";};";
        //{rel1} ^ {rel1, rel2, rel3}
        String patternString3 = "{ (role1: $x, $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString3 = "{(role1: $x, $y);" +
                "$y id " + conceptId(tx, "anotherTwoRoleEntity") + ";};";

        List<Type> possibleTypes = Lists.newArrayList(
                tx.getType(Label.of("anotherTwoRoleBinary")),
                tx.getType(Label.of("threeRoleBinary"))
        );

        typeInference(possibleTypes, patternString, subbedPatternString, (TestTransactionProvider.TestTransaction)tx);
        typeInference(possibleTypes, patternString2, subbedPatternString2, (TestTransactionProvider.TestTransaction)tx);
        typeInference(Collections.singletonList(tx.getSchemaConcept(Label.of("twoRoleBinary"))), patternString3, subbedPatternString3, (TestTransactionProvider.TestTransaction)tx);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_bothConceptsAreSubConcepts() {
        //{rel3} ^ {rel2, rel3}
        String patternString = "{ (subRole2: $x, $y); $y isa twoRoleEntity; };";
        String subbedPatternString = "{(subRole2: $x, $y);" +
                "$y id " + conceptId(tx, "twoRoleEntity") + ";};";
        //{rel3} ^ {rel1, rel2, rel3}
        String patternString2 = "{ (subRole2: $x, $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString2 = "{(subRole2: $x, $y);" +
                "$y id " + conceptId(tx, "anotherTwoRoleEntity") + ";};";

        typeInference(Collections.singletonList(tx.getSchemaConcept(Label.of("threeRoleBinary"))), patternString, subbedPatternString, (TestTransactionProvider.TestTransaction)tx);
        typeInference(Collections.singletonList(tx.getSchemaConcept(Label.of("threeRoleBinary"))), patternString2, subbedPatternString2,(TestTransactionProvider.TestTransaction) tx);
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_typeContradiction() {
        //{rel1} ^ {rel2}
        String patternString = "{ (role1: $x, $y); $y isa singleRoleEntity; };";
        String subbedPatternString = "{(role1: $x, $y);" +
                "$y id " + conceptId(tx, "singleRoleEntity") + ";};";
        String patternString2 = "{ (role1: $x, $y); $x isa singleRoleEntity; };";
        String subbedPatternString2 = "{(role1: $x, $y);" +
                "$x id " + conceptId(tx, "singleRoleEntity") + ";};";

        typeInference(Collections.emptyList(), patternString, subbedPatternString,(TestTransactionProvider.TestTransaction) tx);
        typeInference(Collections.emptyList(), patternString2, subbedPatternString2,(TestTransactionProvider.TestTransaction) tx);
    }

    @Test
    public void testTypeInference_singleRole_doubleGuard() {
        //{rel2, rel3} ^ {rel1, rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString = "{ $x isa singleRoleEntity;(role2: $x, $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString = "{(role2: $x, $y);" +
                "$x id " + conceptId(tx, "singleRoleEntity") + ";" +
                "$y id " + conceptId(tx, "anotherTwoRoleEntity") +";};";

        List<Type> possibleTypes = Lists.newArrayList(
                tx.getType(Label.of("anotherTwoRoleBinary")),
                tx.getType(Label.of("threeRoleBinary"))
        );
        typeInference(possibleTypes, patternString, subbedPatternString,(TestTransactionProvider.TestTransaction) tx);
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard() {
        //{rel1, rel2, rel3} ^ {rel3} ^ {rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString = "{ $x isa threeRoleEntity;(subRole2: $x, role3: $y); $y isa threeRoleEntity; };";
        String subbedPatternString = "{(subRole2: $x, role3: $y);" +
                "$x id " + conceptId(tx, "threeRoleEntity") + ";" +
                "$y id " + conceptId(tx, "threeRoleEntity") + ";};";

        //{rel1, rel2, rel3} ^ {rel1, rel2, rel3} ^ {rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString2 = "{ $x isa threeRoleEntity;(role2: $x, role3: $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString2 = "{(role2: $x, role3: $y);" +
                "$x id " + conceptId(tx, "threeRoleEntity") + ";" +
                "$y id " + conceptId(tx, "anotherTwoRoleEntity") +";};";

        typeInference(Collections.singletonList(tx.getSchemaConcept(Label.of("threeRoleBinary"))), patternString, subbedPatternString,(TestTransactionProvider.TestTransaction) tx);

        List<Type> possibleTypes = Lists.newArrayList(
                tx.getType(Label.of("anotherTwoRoleBinary")),
                tx.getType(Label.of("threeRoleBinary"))
        );
        typeInference(possibleTypes, patternString2, subbedPatternString2,(TestTransactionProvider.TestTransaction) tx);
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard_contradiction() {
        //{rel2, rel3} ^ {rel1} ^ {rel1, rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString = "{ $x isa singleRoleEntity;(role1: $x, role2: $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString = "{(role1: $x, role2: $y);" +
                "$x id " + conceptId(tx, "singleRoleEntity") + ";" +
                "$y id " + conceptId(tx, "anotherTwoRoleEntity") +";};";

        //{rel2, rel3} ^ {rel1} ^ {rel1, rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString2 = "{ $x isa singleRoleEntity;(role1: $x, role2: $y); $y isa anotherSingleRoleEntity; };";
        String subbedPatternString2 = "{(role1: $x, role2: $y);" +
                "$x id " + conceptId(tx, "singleRoleEntity") + ";" +
                "$y id " + conceptId(tx, "anotherSingleRoleEntity") +";};";

        typeInference(Collections.emptyList(), patternString, subbedPatternString, (TestTransactionProvider.TestTransaction)tx);
        typeInference(Collections.emptyList(), patternString2, subbedPatternString2, (TestTransactionProvider.TestTransaction)tx);
    }

    @Test
    public void testTypeInference_metaGuards() {
        String patternString = "{ ($x, $y);$x isa entity; $y isa entity; };";
        typeInference(allRelations(tx), patternString, tx);
    }

    @Test
    public void testTypeInference_genericRelation() {
        String patternString = "{ ($x, $y); };";
        typeInference(allRelations(tx), patternString, tx);
    }

    private <T extends Atomic> T getAtom(ReasonerQuery q, Class<T> type, Set<Variable> vars){
        return q.getAtoms(type)
                .filter(at -> at.getVarNames().containsAll(vars))
                .findFirst().get();
    }

    @Test
    public void testTypeInference_conjunctiveQuery() {
        String patternString = "{" +
                "($x, $y); $x isa anotherSingleRoleEntity;" +
                "($y, $z); $y isa anotherTwoRoleEntity;" +
                "($z, $w); $w isa threeRoleEntity;" +
                "};";

        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        ReasonerQuery conjQuery = reasonerQueryFactory.create(conjunction(patternString));

        //determination of possible rel types for ($y, $z) relation depends on its neighbours which should be preserved
        //when resolving (and separating atoms) the query
        RelationAtom XYatom = getAtom(conjQuery, RelationAtom.class, Sets.newHashSet(new Variable("x"), new Variable("y")));
        RelationAtom YZatom = getAtom(conjQuery, RelationAtom.class, Sets.newHashSet(new Variable("y"), new Variable("z")));
        RelationAtom ZWatom = getAtom(conjQuery, RelationAtom.class, Sets.newHashSet(new Variable("z"), new Variable("w")));
        RelationAtom midAtom = (RelationAtom) reasonerQueryFactory.atomic(YZatom).getAtom();

        assertCollectionsNonTriviallyEqual(midAtom.getPossibleTypes(), YZatom.getPossibleTypes());

        //differently prioritised options arise from using neighbour information
        List<Type> firstTypeOption = Lists.newArrayList(
                tx.getType(Label.of("twoRoleBinary")),
                tx.getType(Label.of("anotherTwoRoleBinary")),
                tx.getType(Label.of("threeRoleBinary"))
        );
        List<Type> secondTypeOption = Lists.newArrayList(
                tx.getType(Label.of("anotherTwoRoleBinary")),
                tx.getType(Label.of("twoRoleBinary")),
                tx.getType(Label.of("threeRoleBinary"))
        );
        typeInference(secondTypeOption, XYatom.getCombinedPattern().toString(), tx);
        typeInference(firstTypeOption, YZatom.getCombinedPattern().toString(), tx);
        typeInference(firstTypeOption, ZWatom.getCombinedPattern().toString(), tx);
    }

    private void typeInference(List<Type> possibleTypes, String pattern, Transaction tx){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        ReasonerAtomicQuery query = reasonerQueryFactory.atomic(conjunction(pattern));
        Atom atom = query.getAtom();
        List<Type> relationTypes = atom.getPossibleTypes();

        if (possibleTypes.size() == 1){
            assertEquals(possibleTypes, relationTypes);
            assertEquals(atom.getSchemaConcept(), Iterables.getOnlyElement(possibleTypes));
        } else {
            GraqlTestUtil.assertCollectionsNonTriviallyEqual(possibleTypes, relationTypes);
            assertEquals(atom.getSchemaConcept(), null);
        }

        typeInferenceQueries(possibleTypes, pattern, (TestTransactionProvider.TestTransaction)tx);
    }

    private void typeInference(List<Type> possibleTypes, String pattern, String subbedPattern, TestTransactionProvider.TestTransaction testTx){
        ReasonerQueryFactory reasonerQueryFactory = testTx.reasonerQueryFactory();

        ReasonerAtomicQuery query = reasonerQueryFactory.atomic(conjunction(pattern));
        ReasonerAtomicQuery subbedQuery = reasonerQueryFactory.atomic(conjunction(subbedPattern));
        Atom atom = query.getAtom();
        Atom subbedAtom = subbedQuery.getAtom();

        List<Type> relationTypes = atom.getPossibleTypes();
        List<Type> subbedRelationTypes = subbedAtom.getPossibleTypes();
        if (possibleTypes.size() == 1){
            assertEquals(possibleTypes, relationTypes);
            assertEquals(relationTypes, subbedRelationTypes);
            assertEquals(atom.getSchemaConcept(), Iterables.getOnlyElement(possibleTypes));
            assertEquals(subbedAtom.getSchemaConcept(), Iterables.getOnlyElement(possibleTypes));
        } else {
            GraqlTestUtil.assertCollectionsEqual(possibleTypes, relationTypes);
            GraqlTestUtil.assertCollectionsEqual(relationTypes, subbedRelationTypes);

            assertNull(atom.getSchemaConcept());
            assertNull(subbedAtom.getSchemaConcept());
        }

        typeInferenceQueries(possibleTypes, pattern, testTx);
        typeInferenceQueries(possibleTypes, subbedPattern, testTx);
    }

    private void typeInferenceQueries(List<Type> possibleTypes, String pattern, TestTransactionProvider.TestTransaction tx) {
        List<ConceptMap> typedAnswers = typedAnswers(possibleTypes, pattern, tx);
        List<ConceptMap> unTypedAnswers = tx.execute(Graql.match(Graql.parsePattern(pattern)).get());
        assertEquals(typedAnswers.size(), unTypedAnswers.size());
        GraqlTestUtil.assertCollectionsEqual(typedAnswers, unTypedAnswers);
    }

    private List<ConceptMap> typedAnswers(List<Type> possibleTypes, String pattern, TestTransactionProvider.TestTransaction tx){
        List<ConceptMap> answers = new ArrayList<>();
        ReasonerAtomicQuery query = tx.reasonerQueryFactory().atomic(conjunction(pattern));
        for(SchemaConcept type : possibleTypes){
            GraqlGet typedQuery = Graql.match(tx.reasonerQueryFactory().atomic(query.getAtom().addType(type)).getPattern()).get();
            tx.stream(typedQuery).filter(ans -> !answers.contains(ans)).forEach(answers::add);
        }
        return answers;
    }

    private List<Type> allRelations(Transaction tx){
        RelationType metaType = tx.getRelationType(Graql.Token.Type.RELATION.toString());
        return metaType.subs().filter(t -> !t.equals(metaType)).collect(Collectors.toList());
    }

    private ConceptId conceptId(Transaction tx, String type){
        return tx.getEntityType(type)
                .instances()
                .filter(instance -> instance.type().label().toString().equals(type))
                .map(Concept::id).findFirst().orElse(null);
    }

    private Conjunction<Statement> conjunction(String patternString){
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}
