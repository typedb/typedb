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
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.common.GraqlTestUtil;
import grakn.core.test.rule.GraknTestStorage;
import grakn.core.test.rule.SessionUtil;
import grakn.core.test.rule.TestTransactionProvider;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static grakn.core.test.common.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class TypeInferenceIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session session;
    private Transaction tx;

    @BeforeClass
    public static void loadContext(){
        Config mockServerConfig = storage.createCompatibleServerConfig();
        session = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);

        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            /*
            People can own dogs and toys and be employed
            pets can own toys, or borrow toys from other pets or people
            Dogs can chew dog-toys
            Companies can be employers only
             */
            tx.execute(Graql.parse("define " +
                    "ownership sub relation," +
                    "  relates owner," +
                    "  relates owned;" +
                    "borrowing sub ownership," +
                    "  relates owner," +
                    "  relates owned," +
                    "  relates borrowed as owned," +
                    "  relates borrower as owner," +
                    "  relates supplier as owner;" +
                    "employment sub relation," +
                    "  relates employer," +
                    "  relates employee;" +
                    "chews sub relation," +
                    "  relates chewed," +
                    "  relates chewer;" +
                    "root-entity sub entity;" +
                    "person sub root-entity," +
                    "  plays owner," +
                    "  plays borrower," +
                    "  plays supplier," +
                    "  plays employee;" +
                    "pet sub root-entity," +
                    "  plays owner," +
                    "  plays owned," +
                    "  plays borrowed," +
                    "  plays borrower," +
                    "  plays supplier;" +
                    "dog sub pet," +
                    "  plays chewer;" +
                    "toy sub root-entity," +
                    "  plays owned," +
                    "  plays borrowed;" +
                    "dog-toy sub toy," +
                    "  plays chewed;" +
                    "company sub root-entity," +
                    "  plays employer;" +
                    "").asDefine());

            tx.execute(Graql.parse("insert" +
                    "$root isa root-entity;" +
                    "$pet isa pet;" +
                    "$dog isa dog;" +
                    "$person isa person;" +
                    "$company isa company; " +
                    "$toy isa toy;" +
                    "$dog-toy isa dog-toy;"
                    ).asInsert());
            tx.commit();
        }
    }

    @AfterClass
    public static void closeSession(){
        session.close();
    }

    @Before
    public void setup() {
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @After
    public void tearDown() {
        tx.close();
    }

    /*
    COllapse a super type and a subtype into the subtype only -- aggregate type information downward
     */
    @Test
    public void whenCalculatingTypeMaps_typesAreCollapsedCorrectly(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        ReasonerQuery query = reasonerQueryFactory.atomic(conjunction("{($x); $x isa pet;$x isa dog;};"));
        assertEquals(tx.getEntityType("dog"), Iterables.getOnlyElement(query.getVarTypeMap().get(new Variable("x"))));

        query = reasonerQueryFactory.atomic(conjunction("{($x); $x isa entity;$x isa pet;};"));
        assertEquals(tx.getType(Label.of("pet")), Iterables.getOnlyElement(query.getVarTypeMap().get(new Variable("x"))));
    }

    /*
    Test type inference using a single role player type to obtain all possible relation types
     */
    @Test
    public void testTypeInference_singleGuard() {
        //parent of all role players so all relations possible
        String patternString = "{ $x isa root-entity; ($x, $y); };";
        String subbedPatternString = "{ $x id " + conceptId(tx, "root-entity") + ";($x, $y); };";

        //pet -> ownership, borrowing
        //sub(pet)=dog -> chewing
        String patternString2 = "{ $x isa pet; ($x, $y); };";
        String subbedPatternString2 = "{ $x id " + conceptId(tx, "pet") + ";($x, $y); };";

        //pet -> chewing
        String patternString3 = "{ $x isa dog; ($x, $y); };";
        String subbedPatternString3 = "{ $x id " + conceptId(tx, "dog") + ";($x, $y); };";

        // note that type inference doesn't return ALL subtypes of the allowed types, it just returns the most super type
        List<Type> possibleTypes = Arrays.asList(
                tx.getType(Label.of("employment")),
                tx.getType(Label.of("ownership")),
                tx.getType(Label.of("chews"))
        );

        List<Type> possibleTypes2 = Arrays.asList(
                tx.getType(Label.of("ownership")),
                tx.getType(Label.of("chews"))
        );

        typeInference(possibleTypes, patternString, subbedPatternString, (TestTransactionProvider.TestTransaction)tx);
        typeInference(possibleTypes2, patternString2, subbedPatternString2, (TestTransactionProvider.TestTransaction)tx);
        typeInference(possibleTypes2, patternString3, subbedPatternString3, (TestTransactionProvider.TestTransaction)tx);
    }

    /*
    Use two role player types to constrain the relation types
     */
    @Test
    public void testTypeInference_doubleGuard() {
        //{ownership, borrowing, employment} ^ {ownership, borrowing, chews} = {ownership, borrowing}
        String patternString1 = "{ $x isa person; ($x, $y); $y isa dog; };";
        String subbedPatternString1 = "{($x, $y);" +
                "$x id " + conceptId(tx, "person") + ";" +
                "$y id " + conceptId(tx, "dog") +";};";

        // note [ ] indicates that this is possible because of downward (isa) inheritance
        //{ownership, borrowing, employment} ^ {employment} = {employment}
        String patternString2 = "{ $x isa person; ($x, $y); $y isa company; };";
        String subbedPatternString2 = "{($x, $y);" +
                "$x id " + conceptId(tx, "person") + ";" +
                "$y id " + conceptId(tx, "company") +";};";

        //{ownership, borrowing, employment} ^ {ownership, borrowing, [chews]} = {employment, borrowing}
        String patternString3 = "{ $x isa person; ($x, $y); $y isa toy; };";
        String subbedPatternString3 = "{($x, $y);" +
                "$x id " + conceptId(tx, "person") + ";" +
                "$y id " + conceptId(tx, "toy") +";};";

        //{ownership, borrowing, [chews]} ^ {ownership, borrowing, [chews]} = {ownership, borrowing, chews}
        String patternString4 = "{ $x isa toy; ($x, $y); $y isa toy; };";
        String subbedPatternString4 = "{($x, $y);" +
                "$x id " + conceptId(tx, "toy") + ";" +
                "$y id " + conceptId(tx, "toy") +";};";

        // type inference only keeps the top-most inferred type
        List<Type> possibleTypesOwnership = Arrays.asList(
                tx.getType(Label.of("ownership"))
//                tx.getType(Label.of("borrowing"))
        );

        List<Type> possibleTypes2 = Arrays.asList(
                tx.getType(Label.of("employment"))
        );

        // type inference only keeps the top-most inferred type
        List<Type> possibleTypes4 = Arrays.asList(
                tx.getType(Label.of("ownership")),
//                tx.getType(Label.of("borrowing"))
                tx.getType(Label.of("chews"))
        );


        typeInference(possibleTypesOwnership, patternString1, subbedPatternString1, (TestTransactionProvider.TestTransaction)tx);
        typeInference(possibleTypes2, patternString2, subbedPatternString2, (TestTransactionProvider.TestTransaction)tx);
        typeInference(possibleTypesOwnership, patternString3, subbedPatternString3, (TestTransactionProvider.TestTransaction)tx);
        typeInference(possibleTypes4, patternString4, subbedPatternString4, (TestTransactionProvider.TestTransaction)tx);
    }

    /*
    A single role type limits the relation types
    In general, we expect a single relation to have a role (the originating one)

    TODO investigate this code path and see if this assumption can simplify code now
     */
    @Test
    public void testTypeInference_singleRole() {
        String patternString = "{ (owner: $x, $y); };";
        String patternString2 = "{ (supplier: $x, $y); };";
        String patternString3 = "{ (chewer: $x, $y); };";

        List<Type> possibleTypes =  Arrays.asList(
                tx.getType(Label.of("ownership")) // we only keep the super, so don't have borrower too
        );
        List<Type> possibleTypes2 =  Arrays.asList(
                tx.getType(Label.of("borrowing"))
        );
        List<Type> possibleTypes3 =  Arrays.asList(
                tx.getType(Label.of("chews"))
        );

        typeInference(possibleTypes, patternString, tx);
        typeInference(possibleTypes2, patternString2, tx);
        typeInference(possibleTypes3, patternString3, tx);
    }

    /*
    A sub role limits the relation type to a single relation type
     */
    @Test
    public void testTypeInference_singleRole_subType() {
        String patternString = "{ (borrower: $x, $y); };";
        typeInference(Arrays.asList(tx.getType(Label.of("borrowing"))), patternString, tx);
    }

    /*
    Combinations of a role type and a player type to constrain a binary relation
     */
    @Test
    public void testTypeInference_singleRole_singleGuard() {

        //{borrowing} ^ {ownership, borrowing, employment} = {borrowing}
        String patternString = "{ (borrower: $x, $y); $y isa person; };";
        String subbedPatternString = "{(borrower: $x, $y);" +
                "$y id " + conceptId(tx, "person") + ";};";
        //{ownership, borrowing} ^ {ownership, borrowing, employment = {ownership, borrowing}
        String patternString2 = "{ (owner: $x, $y); $y isa person; };";
        String subbedPatternString2 = "{(owner: $x, $y);" +
                "$y id " + conceptId(tx, "person") + ";};";
        //{chews} ^ {ownership, borrowing, [chews]} = {chews}
        String patternString3 = "{ (chewed: $x, $y); $y isa pet; };";
        String subbedPatternString3 = "{(chewed: $x, $y);" +
                "$y id " + conceptId(tx, "pet") + ";};";

        List<Type> possibleTypesBorrowing = Arrays.asList(
                tx.getType(Label.of("borrowing"))
        );
        // inference only return topmost type of hierarchy
        List<Type> possibleTypesOwnership = Arrays.asList(
                tx.getType(Label.of("ownership"))
//                tx.getType(Label.of("ownership"))
        );
        List<Type> possibleTypesChews = Arrays.asList(
                tx.getType(Label.of("chews"))
        );
        typeInference(possibleTypesBorrowing, patternString, subbedPatternString, (TestTransactionProvider.TestTransaction)tx);
        typeInference(possibleTypesOwnership, patternString2, subbedPatternString2, (TestTransactionProvider.TestTransaction)tx);
        typeInference(possibleTypesChews, patternString3, subbedPatternString3, (TestTransactionProvider.TestTransaction)tx);
    }

    /*
    Using a subrole and a player that is a subtype to constrain the relation type
     */
    @Test
    public void testTypeInference_singleRole_singleGuard_bothConceptsAreSubConcepts() {
        //{borrowing} ^ {ownership, borrowing, chews}
        String patternString = "{ (borrowed: $x, $y); $y isa dog; };";
        String subbedPatternString = "{(borrower: $x, $y);" +
                "$y id " + conceptId(tx, "dog") + ";};";
        //{borrowing} ^ {ownership, borrowing, chews}
        String patternString2 = "{ (supplier: $x, $y); $y isa dog-toy; };";
        String subbedPatternString2 = "{(supplier: $x, $y);" +
                "$y id " + conceptId(tx, "dog-toy") + ";};";

        typeInference(Arrays.asList(tx.getSchemaConcept(Label.of("borrowing"))), patternString, subbedPatternString, (TestTransactionProvider.TestTransaction)tx);
        typeInference(Arrays.asList(tx.getSchemaConcept(Label.of("borrowing"))), patternString2, subbedPatternString2,(TestTransactionProvider.TestTransaction) tx);
    }

    /*
    A contradiction in role and role player
     */
    @Test
    public void testTypeInference_singleRole_singleGuard_typeContradiction() {
        String patternString = "{ (employee: $x, $y); $y isa dog; };";
        String subbedPatternString = "{(employee: $x, $y);" +
                "$y id " + conceptId(tx, "dog") + ";};";
        String patternString2 = "{ (chewer: $x, $y); $x isa person; };";
        String subbedPatternString2 = "{(chewer: $x, $y);" +
                "$x id " + conceptId(tx, "person") + ";};";

        typeInference(Collections.emptyList(), patternString, subbedPatternString,(TestTransactionProvider.TestTransaction) tx);
        typeInference(Collections.emptyList(), patternString2, subbedPatternString2,(TestTransactionProvider.TestTransaction) tx);
    }

    /*
    constrained binary relation by a role and two role player types
     */
    @Test
    public void testTypeInference_singleRole_doubleGuard() {
        //{ownership, borrowing, employment} ^ {ownership, [borrowing]} ^ {ownership, borrowing, chews}
        String patternString = "{ $x isa person;(owner: $x, $y); $y isa dog; };";
        String subbedPatternString = "{(owner: $x, $y);" +
                "$x id " + conceptId(tx, "person") + ";" +
                "$y id " + conceptId(tx, "dog") +";};";

        // inference only returns topmost inferred type
        List<Type> possibleTypes = Arrays.asList(
                tx.getType(Label.of("ownership"))
//                tx.getType(Label.of("borrowing"))
        );
        typeInference(possibleTypes, patternString, subbedPatternString,(TestTransactionProvider.TestTransaction) tx);
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard() {
        //{ownership, borrowing, employment} ^ {borrowing} ^ {ownership, borrowing} ^ {ownership, borrowing, [chews]}
        String patternString = "{ $x isa person;(supplier: $x, owned: $y); $y isa toy; };";
        String subbedPatternString = "{(supplier: $x, owned: $y);" +
                "$x id " + conceptId(tx, "person") + ";" +
                "$y id " + conceptId(tx, "toy") + ";};";

        //{ownership, borrowing, employment} ^ {ownership, [borrowing]} ^ {ownership, [borrowing]} ^ {ownership, borrowing, [chews]}
        String patternString2 = "{ $x isa person;(owner: $x, owned: $y); $y isa dog-toy; };";
        String subbedPatternString2 = "{(owner: $x, owned: $y);" +
                "$x id " + conceptId(tx, "person") + ";" +
                "$y id " + conceptId(tx, "dog-toy") +";};";

        typeInference(Arrays.asList(tx.getType(Label.of("borrowing"))), patternString, subbedPatternString,(TestTransactionProvider.TestTransaction) tx);
        typeInference(Arrays.asList(tx.getType(Label.of("ownership"))), patternString2, subbedPatternString2,(TestTransactionProvider.TestTransaction) tx);
    }

    @Test
    public void testTypeInference_doubleRole_doubleGuard_contradiction() {
        //{employment} ^ {borrowing} ^ {ownership, borrowing} ^ {ownership, borrowing, [chews]}
        String patternString = "{ $x isa company;(supplier: $x, owned: $y); $y isa toy; };";
        String subbedPatternString = "{(supplier: $x, owned: $y);" +
                "$x id " + conceptId(tx, "company") + ";" +
                "$y id " + conceptId(tx, "toy") +";};";

        typeInference(Collections.emptyList(), patternString, subbedPatternString, (TestTransactionProvider.TestTransaction)tx);
    }

    @Test
    public void testTypeInference_metaGuards() {
        String patternString = "{ ($x, $y);$x isa entity; $y isa entity; };";
        List<Type> allNonMetaRootRelations = Arrays.asList(
                tx.getType(Label.of("ownership")),
                tx.getType(Label.of("chews")),
                tx.getType(Label.of("employment"))
        );
        typeInference(allNonMetaRootRelations, patternString, tx);
    }

    @Test
    public void testTypeInference_genericRelation() {
        String patternString = "{ ($x, $y); };";
        List<Type> allNonMetaRootRelations = Arrays.asList(
                tx.getType(Label.of("ownership")),
                tx.getType(Label.of("chews")),
                tx.getType(Label.of("employment"))
//                tx.getType(Label.of("ownership"))
        );
        typeInference(allNonMetaRootRelations, patternString, tx);
    }

    private <T extends Atomic> T getAtom(ReasonerQuery q, Class<T> type, Set<Variable> vars){
        return q.getAtoms(type)
                .filter(at -> at.getVarNames().containsAll(vars))
                .findFirst().get();
    }

    @Test
    public void testTypeInference_conjunctiveQuery() {

        // first relation is either an ownership or a  borrowing
        // last relation is employment only, so $z must be a company or person
        // which means that the middle relation must be an employment, or ownership or borrowing

        String patternString = "{" +
                "($x, $y); $x isa dog;" +
                "($y, $z); $y isa person;" +
                "($z, $w); $w isa company;" +
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
        List<Type> firstTypeOption = Arrays.asList(
                tx.getType(Label.of("ownership"))
//                tx.getType(Label.of("borrowing"))
        );
        List<Type> secondTypeOption = Arrays.asList(
                tx.getType(Label.of("ownership")),
//                tx.getType(Label.of("borrowing")),
                tx.getType(Label.of("employment"))
        );
        List<Type> thirdTypeOptions = Arrays.asList(
                tx.getType(Label.of("employment"))
        );


        assertCollectionsNonTriviallyEqual(firstTypeOption, XYatom.getPossibleTypes());
        assertCollectionsNonTriviallyEqual(secondTypeOption, YZatom.getPossibleTypes());
        assertCollectionsNonTriviallyEqual(thirdTypeOptions, ZWatom.getPossibleTypes());
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
            assertCollectionsNonTriviallyEqual(possibleTypes, relationTypes);
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
