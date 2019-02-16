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

import com.google.common.collect.Sets;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.query.GraqlGet;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

import static grakn.core.util.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@SuppressWarnings("CheckReturnValue")
public class OntologicalQueryIT {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl genericSchemaSession;

    private static void loadFromFile(String fileName, Session session) {
        try {
            InputStream inputStream = new FileInputStream("test-integration/graql/reasoner/resources/" + fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            Graql.parseList(s).forEach(tx::execute);
            tx.commit();
        } catch (Exception e) {
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    private static TransactionOLTP tx;

    @BeforeClass
    public static void loadContext() {
        genericSchemaSession = server.sessionWithNewKeyspace();
        loadFromFile("ruleApplicabilityTest.gql", genericSchemaSession);
    }

    @AfterClass
    public static void closeSession() {
        genericSchemaSession.close();
    }

    @Before
    public void setUp() {
        tx = genericSchemaSession.transaction(Transaction.Type.WRITE);
    }

    @After
    public void tearDown() {
        tx.close();
    }

//    @Rule
//    public final SampleKBContext matchingTypesContext = SampleKBContext.load("matchingTypesTest.gql");

    //TODO flaky!
//    @Ignore
//    @Test
//    public void instancePairsRelatedToSameTypeOfEntity(){
//        Transaction tx = matchingTypesContext.tx();
//        String basePattern = "$x isa service;" +
//                "$y isa service;" +
//                "(owner: $x, capability: $xx) isa has-capability; $xx isa $type;" +
//                "(owner: $y, capability: $yy) isa has-capability; $yy isa $type;" +
//                "$y != $x;";
//
//        String simpleQuery = "match " +
//                basePattern +
//                "get $x, $y;";
//        String queryWithExclusions = "match " +
//                basePattern +
//                "$meta label entity; $type != $meta;" +
//                "$meta2 label thing; $type != $meta2;" +
//                "$meta3 label capability-type; $type != $meta3;" +
//                "get $x, $y, $type;";
//
//        List<ConceptMap> simpleAnswers = Graql.parse(simpleQuery).asGet().execute(false);
//        List<ConceptMap> simpleAnswersInferred = tx.execute(Graql.parse(simpleQuery).asGet());
//        List<ConceptMap> answersWithExclusions = Graql.parse(queryWithExclusions).asGet().execute(false);
//        List<ConceptMap> answersWithExclusionsInferred = tx.execute(Graql.parse(queryWithExclusions).asGet());
//        assertFalse(simpleAnswers.isEmpty());
//        assertFalse(answersWithExclusions.isEmpty());
//        assertCollectionsNonTriviallyEqual(simpleAnswers, simpleAnswersInferred);
//        assertCollectionsNonTriviallyEqual(answersWithExclusions, answersWithExclusionsInferred);
//    }

    @Test
    public void instancesOfSubsetOfTypesExcludingGivenType() {
        String queryString = "match $x isa $type; $type sub entity; $type2 type noRoleEntity; $type2 != $type; get $x, $type;";

        List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet(), false);
        List<ConceptMap> answersInferred = tx.execute(Graql.parse(queryString).asGet());

        assertFalse(answers.isEmpty());
        assertCollectionsNonTriviallyEqual(answers, answersInferred);
    }

    //TODO need to correctly return THING and RELATIONSHIP mapping for %type
    @Ignore
    @Test
    public void allInstancesAndTheirType() {
                String queryString = "match $x isa $type; get;";

        List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
        assertCollectionsNonTriviallyEqual(answers, tx.execute(Graql.parse(queryString).asGet(), false));
    }

    @Test
    @Ignore //TODO: re-enable this test once we figure out why it randomly fails
    public void allRolePlayerPairsAndTheirRelationType() {
                String relationString = "match $x isa relationship; get;";
        String rolePlayerPairString = "match ($u, $v) isa $type; get;";

        GraqlGet rolePlayerQuery = Graql.parse(rolePlayerPairString).asGet();
        List<ConceptMap> rolePlayerPairs = tx.execute(rolePlayerQuery);
        //TODO doesn't include THING and RELATIONSHIP
        //25 relation variants + 2 x 3 resource relation instances
        assertEquals(31, rolePlayerPairs.size());

        //TODO
        //rolePlayerPairs.forEach(ans -> assertEquals(ans.vars(), rolePlayerQuery.vars()));

        List<ConceptMap> relations = tx.execute(Graql.parse(relationString).asGet());
        //one implicit,
        //3 x binary,
        //2 x ternary,
        //7 (3 reflexive) x reifying-relation
        //3 x has-description resource relation
        assertEquals(16, relations.size());
    }

    /**
     * HasAtom
     **/

    @Test
    public void allInstancesOfTypesThatCanHaveAGivenResourceType() {
                String queryString = "match $x isa $type; $type has name; get;";

        List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
        //1 x noRoleEntity + 3 x 3 (hierarchy) anotherTwoRoleEntities
        assertEquals(10, answers.size());
        assertCollectionsNonTriviallyEqual(answers, tx.execute(Graql.parse(queryString).asGet(), false));
    }

    @Test
    public void allInstancesOfTypesThatCanHaveAGivenResourceType_needInferenceToGetAllResults() {

        String queryString = "match $x isa $type; $type has description; get;";
        String specificQueryString = "match $x isa reifiable-relation;get;";
        List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

        assertEquals(tx.execute(Graql.parse(specificQueryString).asGet()).size() * tx.getRelationshipType("reifiable-relation").subs().count(), answers.size());
        assertCollectionsNonTriviallyEqual(answers, tx.execute(Graql.parse(queryString).asGet(), false));
    }

    /**
     * SubAtom
     **/

    @Test
    public void allInstancesOfTypesThatAreSubTypeOfGivenType() {
                String queryString = "match $x isa $type; $type sub noRoleEntity; get;";

        List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
        assertEquals(tx.getEntityType("noRoleEntity").subs().flatMap(EntityType::instances).count(), answers.size());
        assertCollectionsNonTriviallyEqual(answers, tx.execute(Graql.parse(queryString).asGet(), false));
    }

    @Test
    public void allInstancesOfTypesThatAreSubTypeOfGivenType_needInferenceToGetAllResults() {
                String queryString = "match $x isa $type; $type sub relationship; get;";
        List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

        assertEquals(tx.getRelationshipType("relationship").subs().flatMap(RelationType::instances).count(), answers.size());
        assertCollectionsNonTriviallyEqual(answers, tx.execute(Graql.parse(queryString).asGet(), false));
    }

    @Test
    public void allTypesAGivenTypeSubs() {
                String queryString = "match binary sub $x; get;";
        List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

        assertEquals(
                answers.stream().map(ans -> ans.get("x")).collect(Collectors.toSet()),
                Sets.newHashSet(
                        tx.getSchemaConcept(Label.of("thing")),
                        tx.getSchemaConcept(Label.of("relationship")),
                        tx.getSchemaConcept(Label.of("reifiable-relation")),
                        tx.getSchemaConcept(Label.of("binary"))
                ));
    }

    /**
     * PlaysAtom
     **/

    @Test
    public void allInstancesOfTypesThatPlayGivenRole() {
                String queryString = "match $x isa $type; $type plays someRole; get;";

        List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
        List<ConceptMap> reifiableRelations = tx.execute(Graql.parse("match $x isa reifiable-relation;get;").asGet());
        assertEquals(tx.getEntityType("noRoleEntity").subs().flatMap(EntityType::instances).count() + reifiableRelations.size(), answers.size());
        assertCollectionsNonTriviallyEqual(answers, tx.execute(Graql.parse(queryString).asGet(), false));
    }

    /**
     * RelatesAtom
     **/

    @Test
    public void allInstancesOfRelationsThatRelateGivenRole() {
                String queryString = "match $x isa $type; $type relates someRole; get;";

        List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

        assertCollectionsNonTriviallyEqual(answers, tx.execute(Graql.parse(queryString).asGet(), false));
        List<ConceptMap> relations = tx.execute(Graql.parse("match $x isa relationship;get;").asGet(), false);
        //plus extra 3 cause there are 3 binary relations which are not extra counted as reifiable-relations
        assertEquals(relations.stream().filter(ans -> !ans.get("x").asRelation().type().isImplicit()).count() + 3, answers.size());
    }

    @Test
    public void allRolesGivenRelationRelates() {
                String queryString = "match reifying-relation relates $x; get;";

        List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
        assertEquals(
                tx.getRelationshipType("reifying-relation").roles().collect(Collectors.toSet()),
                answers.stream().map(ans -> ans.get("x")).collect(Collectors.toSet())
        );
    }

    /**
     * IsaAtom
     **/

    @Test
    public void allTypesOfRolePlayerInASpecificRelationWithSpecifiedRoles() {
                String queryString = "match (someRole: $x, subRole: $y) isa reifiable-relation;$x isa $type; get;";

        List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
        //3 instances * {anotherTwoRoleEntity, anotherSingleRoleEntity, noRoleEntity, entity, Thing}
        assertEquals(tx.stream(Graql.parse("match $x isa reifiable-relation; get;").asGet()).count() * 5, answers.size());
    }

    @Test
    public void allTypesOfRolePlayerInASpecificRelationWithUnspecifiedRoles() {
                String queryString = "match ($x, $y) isa reifiable-relation;$x isa $type; get;";

        //3 instances * {anotherTwoRoleEntity, anotherSingleRoleEntity, noRoleEntity, entity, Thing} * arity
        List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
        assertEquals(tx.stream(Graql.parse("match $x isa reifiable-relation; get;").asGet()).count() * 5 * 2, answers.size());
    }

    /**
     * meta concepts
     **/

    @Test
    public void allInstancesOfMetaEntity() {
                long noOfEntities = tx.getMetaEntityType().instances().count();
        String queryString = "match $x isa entity;get;";

        List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
        assertEquals(noOfEntities, answers.size());
    }

    @Test
    public void allInstancesOfMetaRelation() {
                String queryString = "match $x isa relationship;get;";

        List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

        //TODO? doesn't pick up attribute relations
        //one implicit,
        //3 x binary,
        //2 x ternary,
        //7 (3 reflexive) x reifying-relation
        //3 x has-description resource relation
        assertEquals(13, answers.size());
    }

    @Test
    public void allInstancesOfMetaResource() {
                String queryString = "match $x isa attribute;get;";

        List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
        assertEquals(2, answers.size());
    }
}
