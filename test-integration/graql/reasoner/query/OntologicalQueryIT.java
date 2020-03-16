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
 *
 */

package grakn.core.graql.reasoner.query;

import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.stream.Collectors;

import static grakn.core.util.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@SuppressWarnings("CheckReturnValue")
public class OntologicalQueryIT {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private static String resourcePath = "test-integration/graql/reasoner/resources/";

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static Session session;
    
    @BeforeClass
    public static void loadContext(){
        session = server.sessionWithNewKeyspace();
        loadFromFileAndCommit(resourcePath, "ruleApplicabilityTest.gql", session);
    }

    @Test
    public void instancePairsRelatedToSameTypeOfEntity(){
        Session session = server.sessionWithNewKeyspace();
        loadFromFileAndCommit(resourcePath, "matchingTypesTest.gql", session);
        try (Transaction tx = session.writeTransaction()) {
            String basePattern = "$x isa service;" +
                    "$y isa service;" +
                    "(owner: $x, capability: $xx) isa has-capability; $xx isa $type;" +
                    "(owner: $y, capability: $yy) isa has-capability; $yy isa $type;" +
                    "$y != $x;";

            String simpleQuery = "match " +
                    basePattern +
                    "get $x, $y;";
            String queryWithExclusions = "match " +
                    basePattern +
                    "$meta type entity; $type != $meta;" +
                    "$meta2 type thing; $type != $meta2;" +
                    "$meta3 type capability-type; $type != $meta3;" +
                    "get $x, $y, $type;";

            List<ConceptMap> simpleAnswers = tx.execute(Graql.parse(simpleQuery).asGet(), false);
            List<ConceptMap> simpleAnswersInferred = tx.execute(Graql.parse(simpleQuery).asGet());
            List<ConceptMap> answersWithExclusions = tx.execute(Graql.parse(queryWithExclusions).asGet(), false);
            List<ConceptMap> answersWithExclusionsInferred = tx.execute(Graql.parse(queryWithExclusions).asGet());
            assertFalse(simpleAnswers.isEmpty());
            assertFalse(answersWithExclusions.isEmpty());
            assertCollectionsNonTriviallyEqual(simpleAnswers, simpleAnswersInferred);
            assertCollectionsNonTriviallyEqual(answersWithExclusions, answersWithExclusionsInferred);
        }
    }

    @Test
    public void instancesOfSubsetOfTypesExcludingGivenType() {
        try(Transaction tx = session.readTransaction()){
            String queryString = "match $x isa $type; $type sub entity; $type2 type noRoleEntity; $type2 != $type; get $x, $type;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet(), false);
            List<ConceptMap> answersInferred = tx.execute(Graql.parse(queryString).asGet());

            assertFalse(answers.isEmpty());
            assertCollectionsNonTriviallyEqual(answers, answersInferred);
        }
    }

    //TODO need to correctly return THING and RELATION mapping for %type
    @Ignore
    @Test
    public void allInstancesAndTheirType() {
        try(Transaction tx = session.readTransaction()) {
            String queryString = "match $x isa $type; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertCollectionsNonTriviallyEqual(answers, tx.execute(Graql.parse(queryString).asGet(), false));
        }
    }

    @Test
    @Ignore //TODO: re-enable this test once we figure out why it randomly fails
    public void allRolePlayerPairsAndTheirRelationType() {
        try(Transaction tx = session.readTransaction()) {
            String relationString = "match $x isa relation; get;";
            String rolePlayerPairString = "match ($u, $v) isa $type; get;";

            GraqlGet rolePlayerQuery = Graql.parse(rolePlayerPairString).asGet();
            List<ConceptMap> rolePlayerPairs = tx.execute(rolePlayerQuery);
            //TODO doesn't include THING and RELATION
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
    }

    /**
     * HasAtom
     **/

    @Test
    public void allInstancesOfTypesThatCanHaveAGivenResourceType() {
        try(Transaction tx = session.readTransaction()) {
            String queryString = "match $x isa $type; $type has name; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            //1 x noRoleEntity + 3 x 3 (hierarchy) anotherTwoRoleEntities
            assertEquals(10, answers.size());
            assertCollectionsNonTriviallyEqual(answers, tx.execute(Graql.parse(queryString).asGet(), false));
        }
    }

    @Test
    public void allInstancesOfTypesThatCanHaveAGivenResourceType_needInferenceToGetAllResults() {
        try(Transaction tx = session.readTransaction()) {
            String queryString = "match $x isa $type; $type has description; get;";
            String specificQueryString = "match $x isa reifiable-relation;get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

            assertEquals(tx.execute(Graql.parse(specificQueryString).asGet()).size() * tx.getRelationType("reifiable-relation").subs().count(), answers.size());
            assertCollectionsNonTriviallyEqual(answers, tx.execute(Graql.parse(queryString).asGet(), false));
        }
    }

    /**
     * SubAtom
     **/

    @Test
    public void allInstancesOfTypesThatAreSubTypeOfGivenType() {
        try(Transaction tx = session.readTransaction()) {
            String queryString = "match $x isa $type; $type sub noRoleEntity; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(tx.getEntityType("noRoleEntity").subs().flatMap(EntityType::instances).count(), answers.size());
            assertCollectionsNonTriviallyEqual(tx.execute(Graql.parse(queryString).asGet(), false), answers);
        }
    }

    @Test
    public void allInstancesOfTypesThatAreSubTypeOfGivenType_needInferenceToGetAllResults() {
        try(Transaction tx = session.readTransaction()) {
            String queryString = "match $x isa $type; $type sub relation; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

            assertEquals(tx.getRelationType("relation").subs().flatMap(RelationType::instances).count(), answers.size());
            assertCollectionsNonTriviallyEqual(answers, tx.execute(Graql.parse(queryString).asGet(), false));
        }
    }

    @Test
    public void allTypesAGivenTypeSubs() {
        try(Transaction tx = session.readTransaction()) {
            String queryString = "match binary sub $x; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

            assertEquals(
                    answers.stream().map(ans -> ans.get("x")).collect(Collectors.toSet()),
                    Sets.newHashSet(
                            tx.getSchemaConcept(Label.of("thing")),
                            tx.getSchemaConcept(Label.of("relation")),
                            tx.getSchemaConcept(Label.of("reifiable-relation")),
                            tx.getSchemaConcept(Label.of("binary"))
                    ));
        }
    }

    /**
     * PlaysAtom
     **/

    @Test
    public void allInstancesOfTypesThatPlayGivenRole() {
        try(Transaction tx = session.readTransaction()) {
            String queryString = "match $x isa $type; $type plays someRole; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            List<ConceptMap> reifiableRelations = tx.execute(Graql.parse("match $x isa reifiable-relation;get;").asGet());
            assertEquals(tx.getEntityType("noRoleEntity").subs().flatMap(EntityType::instances).count() + reifiableRelations.size(), answers.size());
            assertCollectionsNonTriviallyEqual(answers, tx.execute(Graql.parse(queryString).asGet(), false));
        }
    }

    /**
     * RelatesAtom
     **/

    @Test
    public void allInstancesOfRelationsThatRelateGivenRole() {
        try(Transaction tx = session.readTransaction()) {
            String queryString = "match $x isa $type; $type relates someRole; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

            assertCollectionsNonTriviallyEqual(tx.execute(Graql.parse(queryString).asGet(), false), answers);
            List<ConceptMap> relations = tx.execute(Graql.parse("match $x isa relation;get;").asGet(), false);
            //plus extra 3 cause there are 3 binary relations which are not extra counted as reifiable-relations
            assertEquals(relations.stream().filter(ans -> !ans.get("x").asRelation().type().isImplicit()).count() + 3, answers.size());
        }
    }

    @Test
    public void allRolesGivenRelationRelates() {
        try(Transaction tx = session.readTransaction()) {
            String queryString = "match reifying-relation relates $x; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(
                    tx.getRelationType("reifying-relation").roles().collect(Collectors.toSet()),
                    answers.stream().map(ans -> ans.get("x")).collect(Collectors.toSet())
            );
        }
    }

    /**
     * IsaAtom
     **/

    @Test
    public void allTypesOfRolePlayerInASpecificRelationWithSpecifiedRoles() {
        try(Transaction tx = session.readTransaction()) {
            String queryString = "match (someRole: $x, subRole: $y) isa reifiable-relation;$x isa $type; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            //3 instances * {anotherTwoRoleEntity, anotherSingleRoleEntity, noRoleEntity, entity, Thing}
            assertEquals(tx.stream(Graql.parse("match $x isa reifiable-relation; get;").asGet()).count() * 5, answers.size());
        }
    }

    @Test
    public void allTypesOfRolePlayerInASpecificRelationWithUnspecifiedRoles() {
        try(Transaction tx = session.readTransaction()) {
            String queryString = "match ($x, $y) isa reifiable-relation;$x isa $type; get;";

            //3 instances * {anotherTwoRoleEntity, anotherSingleRoleEntity, noRoleEntity, entity, Thing} * arity
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(tx.stream(Graql.parse("match $x isa reifiable-relation; get;").asGet()).count() * 5 * 2, answers.size());
        }
    }

    /**
     * meta concepts
     **/

    @Test
    public void allInstancesOfMetaEntity() {
        try(Transaction tx = session.readTransaction()) {
            final long noOfEntities = tx.getMetaEntityType().instances().count();
            String queryString = "match $x isa entity;get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(noOfEntities, answers.size());
        }
    }

    @Test
    public void allInstancesOfMetaRelation() {
        try(Transaction tx = session.readTransaction()) {
            String queryString = "match $x isa relation;get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

            //TODO? doesn't pick up attribute relations
            //one implicit,
            //3 x binary,
            //2 x ternary,
            //7 (3 reflexive) x reifying-relation
            //3 x has-description resource relation
            assertEquals(13, answers.size());
        }
    }

    @Test
    public void allInstancesOfMetaResource() {
        try(Transaction tx = session.readTransaction()) {
            String queryString = "match $x isa attribute;get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(2, answers.size());
        }
    }
}
