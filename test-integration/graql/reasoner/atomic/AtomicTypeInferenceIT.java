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

package grakn.core.graql.reasoner.atomic;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.concept.type.Type;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.Atomic;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueries;
import grakn.core.graql.reasoner.query.ReasonerQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.kb.Schema;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import grakn.core.util.GraqlTestUtil;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class AtomicTypeInferenceIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl testContextSession;

    private static void loadFromFile(String fileName, Session session){
        try {
            InputStream inputStream = AtomicTypeInferenceIT.class.getClassLoader().getResourceAsStream("test-integration/graql/reasoner/resources/"+fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            Graql.parseList(s).forEach(tx::execute);
            tx.commit();
        } catch (Exception e){
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void loadContext(){
        testContextSession = server.sessionWithNewKeyspace();
        loadFromFile("typeInferenceTest.gql", testContextSession);
    }

    @AfterClass
    public static void closeSession(){
        testContextSession.close();
    }

    @Test
    public void testTypeInference_singleGuard() {
        TransactionOLTP tx = testContextSession.transaction(Transaction.Type.WRITE);

        //parent of all roles so all relations possible
        String patternString = "{ $x isa noRoleEntity; ($x, $y); };";
        String subbedPatternString = "{ $x id '" + conceptId(tx, "noRoleEntity") + "';($x, $y); };";

        //SRE -> rel2
        //sub(SRE)=TRE -> rel3
        String patternString2 = "{ $x isa singleRoleEntity; ($x, $y); };";
        String subbedPatternString2 = "{ $x id '" + conceptId(tx, "singleRoleEntity") + "';($x, $y); };";

        //TRE -> rel3
        String patternString3 = "{ $x isa twoRoleEntity; ($x, $y); };";
        String subbedPatternString3 = "{ $x id '" + conceptId(tx, "twoRoleEntity") + "';($x, $y); };";

        List<Type> possibleTypes = Lists.newArrayList(
                tx.getType(Label.of("anotherTwoRoleBinary")),
                tx.getType(Label.of("threeRoleBinary"))
        );

        typeInference(allRelations(tx), patternString, subbedPatternString, tx);
        typeInference(possibleTypes, patternString2, subbedPatternString2, tx);
        typeInference(possibleTypes, patternString3, subbedPatternString3, tx);
        tx.close();
    }

    @Test
    public void testTypeInference_doubleGuard() {
        TransactionOLTP tx = testContextSession.transaction(Transaction.Type.WRITE);

        //{rel2, rel3} ^ {rel1, rel2, rel3} = {rel2, rel3}
        String patternString = "{ $x isa singleRoleEntity; ($x, $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString = "{($x, $y);" +
                "$x id '" + conceptId(tx, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(tx, "anotherTwoRoleEntity") +"';};";

        //{rel2, rel3} ^ {rel1, rel2, rel3} = {rel2, rel3}
        String patternString2 = "{ $x isa twoRoleEntity; ($x, $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString2 = "{($x, $y);" +
                "$x id '" + conceptId(tx, "twoRoleEntity") + "';" +
                "$y id '" + conceptId(tx, "anotherTwoRoleEntity") +"';};";

        //{rel1} ^ {rel1, rel2, rel3} = {rel1}
        String patternString3 = "{ $x isa yetAnotherSingleRoleEntity; ($x, $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString3 = "{($x, $y);" +
                "$x id '" + conceptId(tx, "yetAnotherSingleRoleEntity") + "';" +
                "$y id '" + conceptId(tx, "anotherTwoRoleEntity") +"';};";

        List<Type> possibleTypes = Lists.newArrayList(
                tx.getType(Label.of("anotherTwoRoleBinary")),
                tx.getType(Label.of("threeRoleBinary"))
        );

        List<Type> possibleTypes2 = Collections.singletonList(tx.getType(Label.of("twoRoleBinary")));

        typeInference(possibleTypes, patternString, subbedPatternString, tx);
        typeInference(possibleTypes, patternString2, subbedPatternString2, tx);
        typeInference(possibleTypes2, patternString3, subbedPatternString3, tx);
        tx.close();
    }

    @Test
    public void testTypeInference_singleRole() {
        TransactionOLTP tx = testContextSession.transaction(Transaction.Type.WRITE);
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
        tx.close();
    }

    @Test
    public void testTypeInference_singleRole_subType() {
        TransactionOLTP tx = testContextSession.transaction(Transaction.Type.WRITE);
        String patternString = "{ (subRole2: $x, $y); };";
        typeInference(Collections.singletonList(tx.getSchemaConcept(Label.of("threeRoleBinary"))), patternString, tx);
        tx.close();
    }

    @Test
    public void testTypeInference_singleRole_singleGuard() {
        TransactionOLTP tx = testContextSession.transaction(Transaction.Type.WRITE);

        //{rel1, rel2, rel3} ^ {rel2, rel3}
        String patternString = "{ (role2: $x, $y); $y isa singleRoleEntity; };";
        String subbedPatternString = "{(role2: $x, $y);" +
                "$y id '" + conceptId(tx, "singleRoleEntity") + "';};";
        //{rel1, rel2, rel3} ^ {rel2, rel3}
        String patternString2 = "{ (role2: $x, $y); $y isa twoRoleEntity; };";
        String subbedPatternString2 = "{(role2: $x, $y);" +
                "$y id '" + conceptId(tx, "twoRoleEntity") + "';};";
        //{rel1} ^ {rel1, rel2, rel3}
        String patternString3 = "{ (role1: $x, $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString3 = "{(role1: $x, $y);" +
                "$y id '" + conceptId(tx, "anotherTwoRoleEntity") + "';};";

        List<Type> possibleTypes = Lists.newArrayList(
                tx.getType(Label.of("anotherTwoRoleBinary")),
                tx.getType(Label.of("threeRoleBinary"))
        );

        typeInference(possibleTypes, patternString, subbedPatternString, tx);
        typeInference(possibleTypes, patternString2, subbedPatternString2, tx);
        typeInference(Collections.singletonList(tx.getSchemaConcept(Label.of("twoRoleBinary"))), patternString3, subbedPatternString3, tx);
        tx.close();
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_bothConceptsAreSubConcepts() {
        TransactionOLTP tx = testContextSession.transaction(Transaction.Type.WRITE);

        //{rel3} ^ {rel2, rel3}
        String patternString = "{ (subRole2: $x, $y); $y isa twoRoleEntity; };";
        String subbedPatternString = "{(subRole2: $x, $y);" +
                "$y id '" + conceptId(tx, "twoRoleEntity") + "';};";
        //{rel3} ^ {rel1, rel2, rel3}
        String patternString2 = "{ (subRole2: $x, $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString2 = "{(subRole2: $x, $y);" +
                "$y id '" + conceptId(tx, "anotherTwoRoleEntity") + "';};";

        typeInference(Collections.singletonList(tx.getSchemaConcept(Label.of("threeRoleBinary"))), patternString, subbedPatternString, tx);
        typeInference(Collections.singletonList(tx.getSchemaConcept(Label.of("threeRoleBinary"))), patternString2, subbedPatternString2, tx);
        tx.close();
    }

    @Test
    public void testTypeInference_singleRole_singleGuard_typeContradiction() {
        TransactionOLTP tx = testContextSession.transaction(Transaction.Type.WRITE);

        //{rel1} ^ {rel2}
        String patternString = "{ (role1: $x, $y); $y isa singleRoleEntity; };";
        String subbedPatternString = "{(role1: $x, $y);" +
                "$y id '" + conceptId(tx, "singleRoleEntity") + "';};";
        String patternString2 = "{ (role1: $x, $y); $x isa singleRoleEntity; };";
        String subbedPatternString2 = "{(role1: $x, $y);" +
                "$x id '" + conceptId(tx, "singleRoleEntity") + "';};";

        typeInference(Collections.emptyList(), patternString, subbedPatternString, tx);
        typeInference(Collections.emptyList(), patternString2, subbedPatternString2, tx);
        tx.close();
    }

    @Test
    public void testTypeInference_singleRole_doubleGuard() {
        TransactionOLTP tx = testContextSession.transaction(Transaction.Type.WRITE);
        //{rel2, rel3} ^ {rel1, rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString = "{ $x isa singleRoleEntity;(role2: $x, $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString = "{(role2: $x, $y);" +
                "$x id '" + conceptId(tx, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(tx, "anotherTwoRoleEntity") +"';};";

        List<Type> possibleTypes = Lists.newArrayList(
                tx.getType(Label.of("anotherTwoRoleBinary")),
                tx.getType(Label.of("threeRoleBinary"))
        );
        typeInference(possibleTypes, patternString, subbedPatternString, tx);
        tx.close();
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard() {
        TransactionOLTP tx = testContextSession.transaction(Transaction.Type.WRITE);

        //{rel1, rel2, rel3} ^ {rel3} ^ {rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString = "{ $x isa threeRoleEntity;(subRole2: $x, role3: $y); $y isa threeRoleEntity; };";
        String subbedPatternString = "{(subRole2: $x, role3: $y);" +
                "$x id '" + conceptId(tx, "threeRoleEntity") + "';" +
                "$y id '" + conceptId(tx, "threeRoleEntity") + "';};";

        //{rel1, rel2, rel3} ^ {rel1, rel2, rel3} ^ {rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString2 = "{ $x isa threeRoleEntity;(role2: $x, role3: $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString2 = "{(role2: $x, role3: $y);" +
                "$x id '" + conceptId(tx, "threeRoleEntity") + "';" +
                "$y id '" + conceptId(tx, "anotherTwoRoleEntity") +"';};";

        typeInference(Collections.singletonList(tx.getSchemaConcept(Label.of("threeRoleBinary"))), patternString, subbedPatternString, tx);

        List<Type> possibleTypes = Lists.newArrayList(
                tx.getType(Label.of("anotherTwoRoleBinary")),
                tx.getType(Label.of("threeRoleBinary"))
        );
        typeInference(possibleTypes, patternString2, subbedPatternString2, tx);
        tx.close();
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard_contradiction() {
        TransactionOLTP tx = testContextSession.transaction(Transaction.Type.WRITE);

        //{rel2, rel3} ^ {rel1} ^ {rel1, rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString = "{ $x isa singleRoleEntity;(role1: $x, role2: $y); $y isa anotherTwoRoleEntity; };";
        String subbedPatternString = "{(role1: $x, role2: $y);" +
                "$x id '" + conceptId(tx, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(tx, "anotherTwoRoleEntity") +"';};";

        //{rel2, rel3} ^ {rel1} ^ {rel1, rel2, rel3} ^ {rel1, rel2, rel3}
        String patternString2 = "{ $x isa singleRoleEntity;(role1: $x, role2: $y); $y isa anotherSingleRoleEntity; };";
        String subbedPatternString2 = "{(role1: $x, role2: $y);" +
                "$x id '" + conceptId(tx, "singleRoleEntity") + "';" +
                "$y id '" + conceptId(tx, "anotherSingleRoleEntity") +"';};";

        typeInference(Collections.emptyList(), patternString, subbedPatternString, tx);
        typeInference(Collections.emptyList(), patternString2, subbedPatternString2, tx);
        tx.close();
    }

    @Test
    public void testTypeInference_metaGuards() {
        TransactionOLTP tx = testContextSession.transaction(Transaction.Type.WRITE);
        String patternString = "{ ($x, $y);$x isa entity; $y isa entity; };";
        typeInference(allRelations(tx), patternString, tx);
        tx.close();
    }

    @Test
    public void testTypeInference_genericRelation() {
        TransactionOLTP tx = testContextSession.transaction(Transaction.Type.WRITE);
        String patternString = "{ ($x, $y); };";
        typeInference(allRelations(tx), patternString, tx);
        tx.close();
    }

    private <T extends Atomic> T getAtom(ReasonerQuery q, Class<T> type, Set<Variable> vars){
        return q.getAtoms(type)
                .filter(at -> at.getVarNames().containsAll(vars))
                .findFirst().get();
    }

    @Test
    public void testTypeInference_conjunctiveQuery() {
        TransactionOLTP tx = testContextSession.transaction(Transaction.Type.WRITE);
        String patternString = "{" +
                "($x, $y); $x isa anotherSingleRoleEntity;" +
                "($y, $z); $y isa anotherTwoRoleEntity;" +
                "($z, $w); $w isa threeRoleEntity;" +
                "};";

        ReasonerQueryImpl conjQuery = ReasonerQueries.create(conjunction(patternString, tx), tx);

        //determination of possible rel types for ($y, $z) relation depends on its neighbours which should be preserved
        //when resolving (and separating atoms) the query
        RelationAtom XYatom = getAtom(conjQuery, RelationAtom.class, Sets.newHashSet(new Variable("x"), new Variable("y")));
        RelationAtom YZatom = getAtom(conjQuery, RelationAtom.class, Sets.newHashSet(new Variable("y"), new Variable("z")));
        RelationAtom ZWatom = getAtom(conjQuery, RelationAtom.class, Sets.newHashSet(new Variable("z"), new Variable("w")));
        RelationAtom midAtom = (RelationAtom) ReasonerQueries.atomic(YZatom).getAtom();

        assertEquals(midAtom.getPossibleTypes(), YZatom.getPossibleTypes());

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
        tx.close();
    }

    private void typeInference(List<Type> possibleTypes, String pattern, TransactionOLTP tx){
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(pattern, tx), tx);
        Atom atom = query.getAtom();
        List<Type> relationTypes = atom.getPossibleTypes();

        if (possibleTypes.size() == 1){
            assertEquals(possibleTypes, relationTypes);
            assertEquals(atom.getSchemaConcept(), Iterables.getOnlyElement(possibleTypes));
        } else {
            GraqlTestUtil.assertCollectionsNonTriviallyEqual(possibleTypes, relationTypes);
            assertEquals(atom.getSchemaConcept(), null);
        }

        typeInferenceQueries(possibleTypes, pattern, tx);
    }

    private void typeInference(List<Type> possibleTypes, String pattern, String subbedPattern, TransactionOLTP tx){
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(pattern, tx), tx);
        ReasonerAtomicQuery subbedQuery = ReasonerQueries.atomic(conjunction(subbedPattern, tx), tx);
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

        typeInferenceQueries(possibleTypes, pattern, tx);
        typeInferenceQueries(possibleTypes, subbedPattern, tx);
    }

    private void typeInferenceQueries(List<Type> possibleTypes, String pattern, TransactionOLTP tx) {
        List<ConceptMap> typedAnswers = typedAnswers(possibleTypes, pattern, tx);
        List<ConceptMap> unTypedAnswers = tx.execute(Graql.match(Graql.parsePattern(pattern)).get());
        assertEquals(typedAnswers.size(), unTypedAnswers.size());
        GraqlTestUtil.assertCollectionsEqual(typedAnswers, unTypedAnswers);
    }

    private List<ConceptMap> typedAnswers(List<Type> possibleTypes, String pattern, TransactionOLTP tx){
        List<ConceptMap> answers = new ArrayList<>();
        ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(pattern, tx), tx);
        for(SchemaConcept type : possibleTypes){
            GraqlGet typedQuery = Graql.match(ReasonerQueries.atomic(query.getAtom().addType(type)).getPattern()).get();
            tx.stream(typedQuery).filter(ans -> !answers.contains(ans)).forEach(answers::add);
        }
        return answers;
    }

    private List<Type> allRelations(TransactionOLTP tx){
        RelationType metaType = tx.getRelationType(Schema.MetaSchema.RELATION.getLabel().getValue());
        return metaType.subs().filter(t -> !t.equals(metaType)).collect(Collectors.toList());
    }

    private ConceptId conceptId(TransactionOLTP tx, String type){
        return tx.getEntityType(type).instances().map(Concept::id).findFirst().orElse(null);
    }

    private Conjunction<Statement> conjunction(String patternString, TransactionOLTP tx){
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}
