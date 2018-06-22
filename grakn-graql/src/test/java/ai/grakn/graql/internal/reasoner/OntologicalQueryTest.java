/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknTx;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Answer;
import ai.grakn.test.rule.SampleKBContext;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.util.GraqlTestUtil.assertCollectionsEqual;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class OntologicalQueryTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Rule
    public final SampleKBContext testContext = SampleKBContext.load("ruleApplicabilityTest.gql");

    @Rule
    public final SampleKBContext matchingTypesContext = SampleKBContext.load("matchingTypesTest.gql");

    @Test
    public void instancePairsRelatedToSameTypeOfEntity(){
        GraknTx tx = matchingTypesContext.tx();
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
                "$meta label entity; $type != $meta;" +
                "$meta2 label thing; $type != $meta2;" +
                "$meta3 label capability-type; $type != $meta3;" +
                "get $x, $y, $type;";

        List<Answer> simpleAnswers = tx.graql().infer(false).<GetQuery>parse(simpleQuery).execute();
        List<Answer> simpleAnswersInferred = tx.graql().infer(true).<GetQuery>parse(simpleQuery).execute();
        List<Answer> answersWithExclusions = tx.graql().infer(false).<GetQuery>parse(queryWithExclusions).execute();
        List<Answer> answersWithExclusionsInferred = tx.graql().infer(true).<GetQuery>parse(queryWithExclusions).execute();
        assertFalse(simpleAnswers.isEmpty());
        assertFalse(answersWithExclusions.isEmpty());
        assertCollectionsEqual(simpleAnswers, simpleAnswersInferred);
        assertCollectionsEqual(answersWithExclusions, answersWithExclusionsInferred);
    }

    @Test
    public void instancesOfSubsetOfTypesExcludingGivenType(){
        GraknTx tx = testContext.tx();
        String queryString = "match $x isa $type; $type sub entity; $type2 label noRoleEntity; $type2 != $type; get $x, $type;";

        List<Answer> answers = tx.graql().infer(true).<GetQuery>parse(queryString).execute();
        List<Answer> answersInferred = tx.graql().infer(false).<GetQuery>parse(queryString).execute();

        assertFalse(answers.isEmpty());
        assertCollectionsEqual(answers, answersInferred);
    }

    //TODO need to correctly return THING and RELATIONSHIP mapping for %type
    @Ignore
    @Test
    public void allInstancesAndTheirType(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa $type; get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertCollectionsEqual(answers, qb.infer(false).<GetQuery>parse(queryString).execute());
    }

    @Test
    public void allRolePlayerPairsAndTheirRelationType(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String relationString = "match $x isa relationship;get;";
        String rolePlayerPairString = "match ($u, $v) isa $type; get;";

        GetQuery rolePlayerQuery = qb.parse(rolePlayerPairString);
        List<Answer> rolePlayerPairs = rolePlayerQuery.execute();
        //TODO doesn't include THING and RELATIONSHIP
        //25 relation variants + 2 x 3 resource relation instances
        assertEquals(rolePlayerPairs.size(), 31);

        //TODO
        //rolePlayerPairs.forEach(ans -> assertEquals(ans.vars(), rolePlayerQuery.vars()));

        List<Answer> relations = qb.<GetQuery>parse(relationString).execute();
        //one implicit,
        //3 x binary,
        //2 x ternary,
        //7 (3 reflexive) x reifying-relation
        //3 x has-description resource relation
        assertEquals(relations.size(), 16);
    }

    /** HasAtom **/

    @Test
    public void allInstancesOfTypesThatCanHaveAGivenResourceType(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa $type; $type has name; get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        //1 x noRoleEntity + 3 x 3 (hierarchy) anotherTwoRoleEntities
        assertEquals(answers.size(), 10);
        assertCollectionsEqual(answers, qb.infer(false).<GetQuery>parse(queryString).execute());
    }

    @Test
    public void allInstancesOfTypesThatCanHaveAGivenResourceType_needInferenceToGetAllResults(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);

        String queryString = "match $x isa $type; $type has description; get;";
        String specificQueryString = "match $x isa reifiable-relation;get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();

        assertEquals(answers.size(), qb.<GetQuery>parse(specificQueryString).execute().size() * tx.getRelationshipType("reifiable-relation").subs().count());
        assertCollectionsEqual(answers, qb.infer(false).<GetQuery>parse(queryString).execute());
    }

    /** SubAtom **/

    @Test
    public void allInstancesOfTypesThatAreSubTypeOfGivenType(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa $type; $type sub noRoleEntity; get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), tx.getEntityType("noRoleEntity").subs().flatMap(EntityType::instances).count());
        assertCollectionsEqual(answers, qb.infer(false).<GetQuery>parse(queryString).execute());
    }

    @Test
    public void allInstancesOfTypesThatAreSubTypeOfGivenType_needInferenceToGetAllResults(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa $type; $type sub relationship; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();

        assertEquals(answers.size(), tx.getRelationshipType("relationship").subs().flatMap(RelationshipType::instances).count());
        assertCollectionsEqual(answers, qb.infer(false).<GetQuery>parse(queryString).execute());
    }

    @Test
    public void allTypesAGivenTypeSubs(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match binary sub $x; get;";
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();

        assertEquals(
                answers.stream().map(ans -> ans.get("x")).collect(Collectors.toSet()),
                Sets.newHashSet(
                        tx.getSchemaConcept(Label.of("thing")),
                        tx.getSchemaConcept(Label.of("relationship")),
                        tx.getSchemaConcept(Label.of("reifiable-relation")),
                        tx.getSchemaConcept(Label.of("binary"))
                        ));
    }

    /** PlaysAtom **/

    @Test
    public void allInstancesOfTypesThatPlayGivenRole(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa $type; $type plays role1; get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        List<Answer> reifiableRelations = qb.<GetQuery>parse("match $x isa reifiable-relation;get;").execute();
        assertEquals(answers.size(), tx.getEntityType("noRoleEntity").subs().flatMap(EntityType::instances).count() + reifiableRelations.size());
        assertCollectionsEqual(answers, qb.infer(false).<GetQuery>parse(queryString).execute());
    }

    /** RelatesAtom **/

    @Test
    public void allInstancesOfRelationsThatRelateGivenRole(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa $type; $type relates role1; get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();

        assertCollectionsEqual(answers, qb.infer(false).<GetQuery>parse(queryString).execute());
        List<Answer> relations = qb.<GetQuery>parse("match $x isa relationship;get;").execute();
        //plus extra 3 cause there are 3 binary relations which are not extra counted as reifiable-relations
        assertEquals(answers.size(), relations.stream().filter(ans -> !ans.get("x").asRelationship().type().isImplicit()).count() + 3);
    }

    @Test
    public void allRolesGivenRelationRelates(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match reifying-relation relates $x; get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(
                answers.stream().map(ans -> ans.get("x")).collect(Collectors.toSet()),
                tx.getRelationshipType("reifying-relation").relates().collect(Collectors.toSet())
        );
    }

    /** IsaAtom **/

    @Test
    public void allTypesOfRolePlayerInASpecificRelationWithSpecifiedRoles(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match (role1: $x, role2: $y) isa reifiable-relation;$x isa $type; get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        //3 instances * {anotherTwoRoleEntity, anotherSingleRoleEntity, noRoleEntity, entity, Thing}
        assertEquals(answers.size(), qb.<GetQuery>parse("match $x isa reifiable-relation; get;").stream().count() * 5);
    }

    @Test
    public void allTypesOfRolePlayerInASpecificRelationWithUnspecifiedRoles(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match ($x, $y) isa reifiable-relation;$x isa $type; get;";

        //3 instances * {anotherTwoRoleEntity, anotherSingleRoleEntity, noRoleEntity, entity, Thing} * arity
        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), qb.<GetQuery>parse("match $x isa reifiable-relation; get;").stream().count() * 5 * 2);
    }

    /** meta concepts **/

    @Test
    public void allInstancesOfMetaEntity(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        long noOfEntities = tx.admin().getMetaEntityType().instances().count();
        String queryString = "match $x isa entity;get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), noOfEntities);
    }

    @Test
    public void allInstancesOfMetaRelation(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa relationship;get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 13);
    }

    @Test
    public void allInstancesOfMetaResource(){
        GraknTx tx = testContext.tx();
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa attribute;get;";

        List<Answer> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 2);
    }
}
