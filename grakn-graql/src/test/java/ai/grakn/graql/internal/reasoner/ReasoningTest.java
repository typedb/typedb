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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.test.rule.SampleKBContext;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections.CollectionUtils;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.GraqlTestUtil.assertCollectionsEqual;
import static ai.grakn.util.Schema.ImplicitType.HAS;
import static ai.grakn.util.Schema.ImplicitType.HAS_OWNER;
import static ai.grakn.util.Schema.ImplicitType.HAS_VALUE;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.commons.math3.util.CombinatoricsUtils;

/**
 * Suite of tests checking different meanders and aspects of reasoning - full reasoning cycle is being tested.
 */
public class ReasoningTest {

    @ClassRule
    public static final SampleKBContext reflexiveRelation = SampleKBContext.load("reflexiveRelation.gql");

    @ClassRule
    public static final SampleKBContext reflexiveSymmetricRelation = SampleKBContext.load("reflexiveSymmetricRelation.gql");

    @ClassRule
    public static final SampleKBContext typeDerivation = SampleKBContext.load("typeDerivation.gql");

    @ClassRule
    public static final SampleKBContext typeDerivationWithDirect = SampleKBContext.load("typeDerivationWithDirect.gql");

    @ClassRule
    public static final SampleKBContext typeDerivationRelationsWithDirect = SampleKBContext.load("typeDerivationRelationsWithDirect.gql");

    @ClassRule
    public static final SampleKBContext typeDerivationFromAttribute = SampleKBContext.load("typeDerivationFromAttribute.gql");

    @ClassRule
    public static final SampleKBContext typeDerivationFromRelations = SampleKBContext.load("typeDerivationFromRelations.gql");

    @ClassRule
    public static final SampleKBContext freshEntityDerivation = SampleKBContext.load("freshEntityDerivationTest.gql");

    @ClassRule
    public static final SampleKBContext freshEntityDerivationFromRelations = SampleKBContext.load("freshEntityDerivationFromRelations.gql");

    @ClassRule
    public static final SampleKBContext freshRelationDerivation = SampleKBContext.load("freshRelationDerivation.gql");

    @ClassRule
    public static final SampleKBContext appendingRPsContext = SampleKBContext.load("appendingRPsTest.gql");

    @ClassRule
    public static final SampleKBContext resourceAttachment = SampleKBContext.load("resourceAttachment.gql");

    @ClassRule
    public static final SampleKBContext resourcesAsRolePlayers = SampleKBContext.load("resourcesAsRolePlayers.gql");

    @ClassRule
    public static final SampleKBContext test7 = SampleKBContext.load("testSet7.gql");

    @ClassRule
    public static final SampleKBContext test8 = SampleKBContext.load("testSet8.gql");

    @ClassRule
    public static final SampleKBContext test9 = SampleKBContext.load("testSet9.gql");

    @ClassRule
    public static final SampleKBContext test10 = SampleKBContext.load("testSet10.gql");

    @ClassRule
    public static final SampleKBContext test11 = SampleKBContext.load("testSet11.gql");

    @ClassRule
    public static final SampleKBContext test12 = SampleKBContext.load("testSet12.gql");

    @ClassRule
    public static final SampleKBContext test13 = SampleKBContext.load("testSet13.gql");

    @ClassRule
    public static final SampleKBContext test19 = SampleKBContext.load("testSet19.gql");

    @ClassRule
    public static final SampleKBContext test19recursive = SampleKBContext.load("testSet19-recursive.gql");

    @ClassRule
    public static final SampleKBContext test20 = SampleKBContext.load("testSet20.gql");

    @ClassRule
    public static final SampleKBContext test21 = SampleKBContext.load("testSet21.gql");

    @ClassRule
    public static final SampleKBContext test22 = SampleKBContext.load("testSet22.gql");

    @ClassRule
    public static final SampleKBContext test23 = SampleKBContext.load("testSet23.gql");

    @ClassRule
    public static final SampleKBContext test24 = SampleKBContext.load("testSet24.gql");

    @ClassRule
    public static final SampleKBContext test25 = SampleKBContext.load("testSet25.gql");

    @ClassRule
    public static final SampleKBContext test26 = SampleKBContext.load("testSet26.gql");

    @ClassRule
    public static final SampleKBContext test27 = SampleKBContext.load("testSet27.gql");

    @ClassRule
    public static final SampleKBContext test28 = SampleKBContext.load("testSet28.gql");

    @ClassRule
    public static final SampleKBContext test28b = SampleKBContext.load("testSet28b.gql");

    @ClassRule
    public static final SampleKBContext test29 = SampleKBContext.load("testSet29.gql");

    @ClassRule
    public static final SampleKBContext test30 = SampleKBContext.load("testSet30.gql");

    @ClassRule
    public static final SampleKBContext resourceOwnership = SampleKBContext.load("resourceOwnershipTest.gql");

    @ClassRule
    public static final SampleKBContext resourceHierarchy = SampleKBContext.load("resourceHierarchy.gql");

    //The tests validate the correctness of the rule reasoning implementation w.r.t. the intended semantics of rules.
    //The ignored tests reveal some bugs in the reasoning algorithm, as they don't return the expected results,
    //as specified in the respective comments below.

    @Test
    public void resourceHierarchiesAreRespected() {
        EmbeddedGraknTx<?> tx = resourceHierarchy.tx();
        QueryBuilder qb = tx.graql().infer(true);

        Set<RelationshipType> relTypes = tx.getMetaRelationType().subs().collect(toSet());
        List<ConceptMap> attributeSubs = qb.<GetQuery>parse("match $x sub attribute; get;").execute();
        List<ConceptMap> attributeRelationSubs = qb.<GetQuery>parse("match $x sub @has-attribute; get;").execute();

        assertEquals(attributeSubs.size(), attributeRelationSubs.size());
        assertTrue(attributeRelationSubs.stream().map(ans -> ans.get("x")).map(Concept::asRelationshipType).allMatch(relTypes::contains));

        List<ConceptMap> baseResourceSubs = qb.<GetQuery>parse("match $x sub baseResource; get;").execute();
        List<ConceptMap> baseResourceRelationSubs = qb.<GetQuery>parse("match $x sub @has-baseResource; get;").execute();
        assertEquals(baseResourceSubs.size(), baseResourceRelationSubs.size());

        assertEquals(
                Sets.newHashSet(
                        tx.getAttributeType("extendedResource"),
                        tx.getAttributeType("anotherExtendedResource"),
                        tx.getAttributeType("furtherExtendedResource"),
                        tx.getAttributeType("simpleResource")
                ),
                tx.getEntityType("genericEntity").attributes().collect(toSet())
        );
    }

    @Test
    public void resourceOwnershipNotPropagatedWithinRelation() {
        EmbeddedGraknTx<?> tx = resourceOwnership.tx();
        QueryBuilder qb = tx.graql().infer(true);

        String attributeName = "name";
        String queryString = "match $x has " + attributeName + " $y; get;";

        String implicitQueryString = "match " +
                "(" +
                HAS_OWNER.getLabel(attributeName).getValue() + ": $x, " +
                HAS_VALUE.getLabel(attributeName).getValue() + ": $y " +
                ") isa " + HAS.getLabel(attributeName).getValue() + ";get;";

        List<ConceptMap> implicitAnswers = qb.<GetQuery>parse(implicitQueryString).execute();
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();

        assertThat(answers, empty());
        assertCollectionsEqual(implicitAnswers, answers);
    }

    @Test //Expected result: Both queries should return a non-empty result, with $x/$y mapped to a unique entity.
    public void unificationOfReflexiveRelations() {
        QueryBuilder qb = reflexiveRelation.tx().graql().infer(true);
        String queryString = "match (role1:$x, role2:$x) isa relation1; get;";
        String queryString2 = "match (role1:$x, role2:$y) isa relation1; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();

        assertEquals(1, answers.size());
        assertEquals(4, answers2.size());
        assertNotEquals(0, answers.size() * answers2.size());
        answers.forEach(x -> assertEquals(1, x.size()));
        answers2.forEach(x -> assertEquals(2, x.size()));
    }

    @Test //Expected result: Both queries should return a non-empty result, with $x/$y mapped to a unique entity.
    public void unificationOfReflexiveSymmetricRelations() {
        QueryBuilder qb = reflexiveSymmetricRelation.tx().graql().infer(true);
        String queryString = "match (symmetricRole: $x, symmetricRole: $x) isa relation1; get;";
        String queryString2 = "match (symmetricRole: $x, symmetricRole: $y) isa relation1; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();

        assertEquals(2, answers.size());
        assertEquals(8, answers2.size());
        assertNotEquals(0, answers.size() * answers2.size());
        answers.forEach(x -> assertEquals(1, x.size()));
        answers2.forEach(x -> assertEquals(2, x.size()));
    }

    @Test //Expected result: The query should return a unique match.
    public void generatingMultipleIsaEdges() {
        QueryBuilder qb = typeDerivation.tx().graql().infer(true);
        String queryString = "match $x isa derivedEntity; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(1, answers.size());
    }

    @Test //Expected result: Differentiated behaviour based on directedness of the isa.
    public void generatingIsaEdgesDirectly() {
        QueryBuilder qb = typeDerivationWithDirect.tx().graql().infer(true);
        String queryString = "match $x isa derivedEntity; get;";
        String queryString2 = "match $x isa! derivedEntity; get;";
        String queryString3 = "match $x isa directDerivedEntity; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
        List<ConceptMap> answers3 = qb.<GetQuery>parse(queryString3).execute();
        assertEquals(2, answers.size());
        assertEquals(2, answers2.size());
        assertEquals(1, answers3.size());
    }

    @Test //Expected result: Differentiated behaviour based on directedness of the isa.
    public void generatingIsaEdgesForRelationsDirectly() {
        QueryBuilder qb = typeDerivationRelationsWithDirect.tx().graql().infer(true);
        String queryString = "match ($x, $y) isa derivedRelation; get;";
        String queryString2 = "match ($x, $y) isa! derivedRelation; get;";
        String queryString3 = "match ($x, $y) isa directDerivedRelation; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
        List<ConceptMap> answers3 = qb.<GetQuery>parse(queryString3).execute();
        assertEquals(2, answers.size());
        assertEquals(2, answers2.size());
        assertEquals(1, answers3.size());
    }

    @Test //Expected result: The query should return 3 results: one for meta type, one for db, one for inferred type.
    public void queryingForGenericType_ruleDefinesNewType() {
        QueryBuilder qb = typeDerivation.tx().graql().infer(true);
        String queryString = "match $x isa $type; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(4, answers.size());
        answers.forEach(ans -> assertEquals(2, ans.size()));
    }

    @Test //Expected result: The queries should return the same two matches.
    public void generatingIsaEdgeFromRelations() {
        QueryBuilder qb = typeDerivationFromRelations.tx().graql().infer(true);
        String queryString = "match $x isa baseEntity; get;";
        String queryString2 = "match $x isa derivedEntity; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(2, answers.size());
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    @Test //Expected result: The queries should return the same two matches.
    public void generatingIsaEdgeFromAttribute() {
        QueryBuilder qb = typeDerivationFromAttribute.tx().graql().infer(true);
        String queryString = "match $x isa baseEntity; get;";
        String queryString2 = "match $x isa derivedEntity; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(typeDerivationFromAttribute.tx().getAttributeType("baseAttribute").instances().count(), answers.size());
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    //TODO: currently disallowed by rule validation
    @Ignore
    @Test //Expected result: The queries should return different matches, unique per query.
    public void generatingFreshEntity() {
        QueryBuilder qb = freshEntityDerivation.tx().graql().infer(true);
        String queryString = "match $x isa baseEntity; get;";
        String queryString2 = "match $x isa derivedEntity; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(answers.size(), answers2.size());
        assertFalse(answers.containsAll(answers2));
        assertFalse(answers2.containsAll(answers));
    }

    //TODO: currently disallowed by rule validation
    @Ignore
    @Test //Expected result: The query should return a unique match (or possibly nothing if we enforce range-restriction).
    public void generatingFreshEntity2() {
        QueryBuilder qb = freshEntityDerivationFromRelations.tx().graql().infer(false);
        QueryBuilder iqb = freshEntityDerivationFromRelations.tx().graql().infer(true);
        String queryString = "match $x isa derivedEntity; get;";
        String explicitQuery = "match $x isa baseEntity; get;";
        List<ConceptMap> answers = iqb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> answers2 = qb.<GetQuery>parse(explicitQuery).execute();

        assertEquals(3, answers2.size());
        assertTrue(!answers2.containsAll(answers));
    }

    //TODO: currently disallowed by rule validation
    @Ignore
    @Test //Expected result: The query should return three different instances of relation1 with unique ids.
    public void generatingFreshRelation() {
        QueryBuilder qb = freshRelationDerivation.tx().graql().infer(true);
        String queryString = "match $x isa baseRelation; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(3, answers.size());
    }

    @Test //Expected result: The query should return 10 unique matches (no duplicates).
    public void distinctLimitedAnswersOfInfinitelyGeneratingRule() {
        QueryBuilder iqb = test7.tx().graql().infer(true);
        QueryBuilder qb = test7.tx().graql().infer(false);
        String queryString = "match $x isa relation1; limit 10; get;";
        List<ConceptMap> answers = iqb.<GetQuery>parse(queryString).execute();
        assertEquals(10, answers.size());
        assertEquals(qb.<GetQuery>parse(queryString).execute().size(), answers.size());
    }

    @Test //Expected result: The query should not return any matches (or possibly return a single match with $x=$y)
    public void roleUnificationWithRoleHierarchiesInvolved() {
        QueryBuilder qb = test8.tx().graql().infer(true);
        String queryString = "match (role2:$x, role3:$y) isa relation2; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertThat(answers, empty());
    }

    @Test //Expected result: The query should not return any matches (or possibly return a single match with $x=$y)
    public void roleUnificationWithRepeatingRoleTypes() {
        QueryBuilder qb = test9.tx().graql().infer(true);
        String queryString = "match (role1:$x, role1:$y) isa relation2; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertThat(answers, empty());
    }

    @Test //Expected result: The query should return a single match
    public void roleUnificationWithLessRelationPlayersInQueryThanHead() {
        QueryBuilder qb = test9.tx().graql().infer(true);
        String queryString = "match (role1:$x) isa relation2; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(1, answers.size());
    }

    /**
     * recursive relation having same type for different role players
     * tests for handling recursivity and equivalence of queries and relations
     */
    @Test //Expected result: The query should return a unique match
    public void transRelationWithEntityGuardsAtBothEnds() {
        QueryBuilder qb = test10.tx().graql().infer(true);
        String queryString = "match (role1: $x, role2: $y) isa relation2; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(1, answers.size());
    }

    @Test //Expected result: The query should return a unique match
    public void transRelationWithRelationGuardsAtBothEnds() {
        QueryBuilder qb = test11.tx().graql().infer(true);
        String queryString = "match (role1:$x, role2:$y) isa relation3; get;";
        assertEquals(1, qb.<GetQuery>parse(queryString).execute().size());
    }

    @Test //Expected result: The query should return two unique matches
    public void circularRuleDependencies() {
        QueryBuilder qb = test12.tx().graql().infer(true);
        String queryString = "match (role1:$x, role2:$y) isa relation3; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(2, answers.size());
    }

    @Test //Expected result: The query should return a unique match
    public void rulesInteractingWithTypeHierarchy() {
        QueryBuilder qb = test13.tx().graql().infer(true);
        String queryString = "match (role1:$x, role2:$y) isa relation2; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(1, answers.size());
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_reattachingResourceToEntity() {
        QueryBuilder qb = resourceAttachment.tx().graql().infer(true);

        String queryString = "match $x isa genericEntity, has reattachable-resource-string $y; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        String queryString2 = "match $x isa reattachable-resource-string; get;";
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();

        assertEquals(resourceAttachment.tx().getEntityType("genericEntity").instances().count(), answers.size());
        assertEquals(1, answers2.size());
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_queryingForGenericRelation() {
        QueryBuilder qb = resourceAttachment.tx().graql().infer(true);

        String queryString = "match $x isa genericEntity;($x, $y); get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();

        assertEquals(3, answers.size());
        assertEquals(answers.stream().filter(answer -> answer.get("y").isAttribute()).count(), 2);
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_usingExistingResourceToDefineSubResource() {
        QueryBuilder qb = resourceAttachment.tx().graql().infer(true);
        String queryString = "match $x isa genericEntity, has subResource $y; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(resourceAttachment.tx().getEntityType("genericEntity").instances().count(), answers.size());

        String queryString2 = "match $x isa subResource; get;";
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(1, answers2.size());
        assertTrue(answers2.iterator().next().get(var("x")).isAttribute());

        String queryString3 = "match $x isa reattachable-resource-string; $y isa subResource;get;";
        List<ConceptMap> answers3 = qb.<GetQuery>parse(queryString3).execute();
        assertEquals(1, answers3.size());

        assertTrue(answers3.iterator().next().get(var("x")).isAttribute());
        assertTrue(answers3.iterator().next().get(var("y")).isAttribute());
    }

    //TODO leads to cache inconsistency
    @Ignore
    @Test
    public void whenReasoningWithResourcesWithRelationVar_ResultsAreComplete() {
        QueryBuilder qb = resourceAttachment.tx().graql().infer(true);

        VarPattern has = var("x").has(Label.of("reattachable-resource-string"), var("y"), var("r"));
        List<ConceptMap> answers = qb.match(has).get().execute();
        assertEquals(3, answers.size());
        answers.forEach(a -> assertTrue(a.vars().contains(var("r"))));
    }

    @Test
    public void whenExecutingAQueryWithImplicitTypes_InferenceHasAtLeastAsManyResults() {
        QueryBuilder withInference = resourceAttachment.tx().graql().infer(true);
        QueryBuilder withoutInference = resourceAttachment.tx().graql().infer(false);

        VarPattern owner = label(HAS_OWNER.getLabel("reattachable-resource-string"));
        VarPattern value = label(HAS_VALUE.getLabel("reattachable-resource-string"));
        VarPattern hasRes = label(HAS.getLabel("reattachable-resource-string"));

        Function<QueryBuilder, GetQuery> query = qb -> qb.match(
                var().rel(owner, "x").rel(value, "y").isa(hasRes),
                var("a").has("reattachable-resource-string", var("b"))  // This pattern is added only to encourage reasoning to activate
        ).get();


        Set<ConceptMap> resultsWithoutInference = query.apply(withoutInference).stream().collect(toSet());
        Set<ConceptMap> resultsWithInference = query.apply(withInference).stream().collect(toSet());

        assertThat(resultsWithoutInference, not(empty()));
        assertThat(Sets.difference(resultsWithoutInference, resultsWithInference), empty());
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_attachingExistingResourceToARelation() {
        QueryBuilder qb = resourceAttachment.tx().graql().infer(true);

        String queryString = "match $x isa genericEntity, has reattachable-resource-string $y; $z isa relation; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(2, answers.size());
        answers.forEach(ans ->
                {
                    assertTrue(ans.get(var("x")).isEntity());
                    assertTrue(ans.get(var("y")).isAttribute());
                    assertTrue(ans.get(var("z")).isRelationship());
                }
        );

        String queryString2 = "match $x isa relation, has reattachable-resource-string $y; get;";
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(1, answers2.size());
        answers2.forEach(ans ->
                {
                    assertTrue(ans.get(var("x")).isRelationship());
                    assertTrue(ans.get(var("y")).isAttribute());
                }
        );
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_derivingResourceFromOtherResourceWithConditionalValue() {
        QueryBuilder qb = resourceAttachment.tx().graql().infer(true);
        String queryString = "match $x has derived-resource-boolean $r; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(1, answers.size());
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void derivingResourceWithSpecificValue() {
        QueryBuilder qb = resourceAttachment.tx().graql().infer(true);
        String queryString = "match $x has derived-resource-string 'value'; get;";
        String queryString2 = "match $x has derived-resource-string $r; get;";
        GetQuery query = qb.parse(queryString);
        GetQuery query2 = qb.parse(queryString2);
        List<ConceptMap> answers = query.execute();
        List<ConceptMap> answers2 = query2.execute();
        List<ConceptMap> requeriedAnswers = query.execute();
        assertEquals(2, answers.size());
        assertEquals(4, answers2.size());
        assertEquals(answers.size(), requeriedAnswers.size());
        assertTrue(answers.containsAll(requeriedAnswers));
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void derivingResource_requireNotHavingSpecificValue() {
        QueryBuilder qb = resourceAttachment.tx().graql().infer(true);
        String queryString = "match " +
                "$x has derived-resource-string $val !== 'unattached';" +
                "get;";
        String queryString2 = "match " +
                "$x has derived-resource-string $val;" +
                "$unwanted 'unattached';" +
                "$val !== $unwanted; get;";

        String complementQueryString = "match $x has derived-resource-string $val 'unattached'; get;";
        String completeQueryString = "match $x has derived-resource-string $val; get;";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> answersBis = qb.<GetQuery>parse(queryString2).execute();

        List<ConceptMap> complement = qb.<GetQuery>parse(complementQueryString).execute();
        List<ConceptMap> complete = qb.<GetQuery>parse(completeQueryString).execute();
        List<ConceptMap> expectedAnswers = ReasonerUtils.listDifference(complete, complement);

        assertCollectionsEqual(expectedAnswers, answers);
        assertCollectionsEqual(expectedAnswers, answersBis);
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void derivingResource_requireValuesToBeDifferent() {
        QueryBuilder qb = resourceAttachment.tx().graql().infer(true);
        String queryString = "match " +
                "$x has derived-resource-string $val;" +
                "$y has reattachable-resource-string $anotherVal;" +
                "$val !== $anotherVal;" +
                "get;";

        qb.<GetQuery>parse(queryString).stream().forEach(ans -> assertNotEquals(ans.get("val"), ans.get("anotherVal")));
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void derivingResourceNotHavingSpecificValue2() {
        QueryBuilder qb = resourceAttachment.tx().graql().infer(true);
        String queryString = "match " +
                "$x has derivable-resource-string $value;" +
                "$x has derivable-resource-string $unwantedValue;" +
                "$unwantedValue 'unattached';" +
                "$value !== $unwantedValue;" +
                "$value isa $type;" +
                "$unwantedValue isa $type;" +
                "$type != $unwantedType;" +
                "$unwantedType label 'derivable-resource-string';" +
                "get;";
        GetQuery query = qb.parse(queryString);
        List<ConceptMap> execute = query.execute();
        System.out.println();
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_attachingStrayResourceToEntityDoesntThrowErrors() {
        QueryBuilder qb = resourceAttachment.tx().graql().infer(true);
        String queryString = "match $x isa yetAnotherEntity, has derived-resource-string 'unattached'; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(2, answers.size());
    }

    @Test
    public void resourcesAsRolePlayers() {
        QueryBuilder qb = resourcesAsRolePlayers.tx().graql().infer(true);

        String queryString = "match $x isa resource 'partial bad flag'; ($x, resource-owner: $y) isa resource-relation; get;";
        String queryString2 = "match $x isa resource 'partial bad flag 2'; ($x, resource-owner: $y) isa resource-relation; get;";
        String queryString3 = "match $x isa resource 'bad flag' ; ($x, resource-owner: $y) isa resource-relation; get;";
        String queryString4 = "match $x isa resource 'no flag' ; ($x, resource-owner: $y) isa resource-relation; get;";
        String queryString5 = "match $x isa resource; ($x, resource-owner: $y) isa resource-relation; get;";
        String queryString6 = "match $x isa resource; $x contains 'bad flag';($x, resource-owner: $y) isa resource-relation; get;";

        GetQuery query = qb.parse(queryString);
        GetQuery query2 = qb.parse(queryString2);
        GetQuery query3 = qb.parse(queryString3);
        GetQuery query4 = qb.parse(queryString4);
        GetQuery query5 = qb.parse(queryString5);
        GetQuery query6 = qb.parse(queryString6);

        List<ConceptMap> answers = query.execute();
        List<ConceptMap> answers2 = query2.execute();
        List<ConceptMap> answers3 = query3.execute();
        List<ConceptMap> answers4 = query4.execute();
        List<ConceptMap> answers5 = query5.execute();
        List<ConceptMap> answers6 = query6.execute();

        assertEquals(answers.size(), 2);
        assertEquals(answers2.size(), 1);
        assertEquals(answers3.size(), 1);
        assertEquals(answers4.size(), 1);
        assertEquals(answers5.size(), answers.size() + answers2.size() + answers3.size() + answers4.size());
        assertEquals(answers6.size(), answers5.size() - answers4.size());
    }

    @Test
    public void resourcesAsRolePlayers_vpPropagationTest() {
        QueryBuilder qb = resourcesAsRolePlayers.tx().graql().infer(true);

        String queryString = "match $x isa resource 'partial bad flag'; ($x, resource-owner: $y) isa another-resource-relation; get;";
        String queryString2 = "match $x isa resource 'partial bad flag 2'; ($x, resource-owner: $y) isa another-resource-relation; get;";
        String queryString3 = "match $x isa resource 'bad flag' ; ($x, resource-owner: $y) isa another-resource-relation; get;";
        String queryString4 = "match $x isa resource 'no flag' ; ($x, resource-owner: $y) isa another-resource-relation; get;";
        String queryString5 = "match $x isa resource; ($x, resource-owner: $y) isa another-resource-relation; get;";
        String queryString6 = "match $x isa resource; $x contains 'bad flag';($x, resource-owner: $y) isa another-resource-relation; get;";

        GetQuery query = qb.parse(queryString);
        GetQuery query2 = qb.parse(queryString2);
        GetQuery query3 = qb.parse(queryString3);
        GetQuery query4 = qb.parse(queryString4);
        GetQuery query5 = qb.parse(queryString5);
        GetQuery query6 = qb.parse(queryString6);

        List<ConceptMap> answers = query.execute();
        List<ConceptMap> answers2 = query2.execute();
        List<ConceptMap> answers3 = query3.execute();
        List<ConceptMap> answers4 = query4.execute();
        List<ConceptMap> answers5 = query5.execute();
        List<ConceptMap> answers6 = query6.execute();

        assertEquals(answers.size(), 3);
        assertEquals(answers2.size(), 3);
        assertEquals(answers3.size(), 3);
        assertEquals(answers4.size(), 3);
        assertEquals(answers5.size(), answers.size() + answers2.size() + answers3.size() + answers4.size());
        assertEquals(answers6.size(), answers5.size() - answers4.size());
    }

    @Test //Expected result: Two answers obtained only if the rule query containing sub type is correctly executed.
    public void instanceTypeHierarchyRespected_queryHasSuperTypes(){
        QueryBuilder qb = test19.tx().graql().infer(true);
        String queryString = "match " +
                "$x isa entity1;" +
                "$y isa entity1;" +
                "(role1: $x, role2: $y) isa relation1;";
        String queryString2 = queryString + "$y has name 'a';";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString + " get;").execute();
        assertEquals(answers.size(), 2);
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2 + " get;").execute();
        assertEquals(answers2.size(), 2);
    }

    @Test //Expected result: Single answer obtained only if the rule query containing super type is correctly executed.
    public void instanceTypeHierarchyRespected_querySpecialisesType(){
        QueryBuilder qb = test19.tx().graql().infer(true);
        String queryString = "match " +
                "$x isa entity1;" +
                "$y isa subEntity1;" +
                "(role1: $x, role2: $y) isa relation1;";
        String queryString2 = queryString + "$y has name 'a';";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString + " get;").execute();
        assertEquals(answers.size(), 1);
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2 + " get;").execute();
        assertEquals(answers2.size(), 1);
    }

    @Test //Expected result: Single answer obtained only if the rule query containing super type is correctly executed.
    public void instanceTypeHierarchyRespected_queryOverwritesTypes(){
        QueryBuilder qb = test19.tx().graql().infer(true);
        String queryString = "match " +
                "$x isa subEntity1;" +
                "$y isa entity1;" +
                "(role1: $x, role2: $y) isa relation1;";
        String queryString2 = queryString + "$y has name 'a';";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString + "get;").execute();
        assertEquals(answers.size(), 2);
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2 + "get;").execute();
        assertEquals(answers2.size(), 2);
    }

    @Test //Expected result: Two answers obtained only if the rule query containing sub type is correctly executed.
    public void instanceTypeHierarchyRespected_queryHasSuperTypes_recursiveRule(){
        QueryBuilder qb = test19recursive.tx().graql().infer(true);
        String queryString = "match " +
                "$x isa entity1;" +
                "$y isa entity1;" +
                "(role1: $x, role2: $y) isa relation1;";
        String queryString2 = queryString + "$y has name 'a';";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString + " get;").execute();
        assertEquals(answers.size(), 2);
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2 + " get;").execute();
        assertEquals(answers2.size(), 2);
    }

    @Test //Expected result: Single answer obtained only if the rule query containing super type is correctly executed.
    public void instanceTypeHierarchyRespected_querySpecialisesType_recursiveRule(){
        QueryBuilder qb = test19recursive.tx().graql().infer(true);
        String queryString = "match " +
                "$x isa entity1;" +
                "$y isa subEntity1;" +
                "(role1: $x, role2: $y) isa relation1;";
        String queryString2 = queryString + "$y has name 'a';";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString + " get;").execute();
        assertEquals(answers.size(), 1);
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2 + " get;").execute();
        assertEquals(answers2.size(), 1);
    }

    @Test //Expected result: Single answer obtained only if the rule query containing super type is correctly executed.
    public void instanceTypeHierarchyRespected_queryOverwritesTypes_recursiveRule(){
        QueryBuilder qb = test19recursive.tx().graql().infer(true);
        String queryString = "match " +
                "$x isa subEntity1;" +
                "$y isa entity1;" +
                "(role1: $x, role2: $y) isa relation1;";
        String queryString2 = queryString + "$y has name 'a';";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString + " get;").execute();
        assertEquals(answers.size(), 2);
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2 + " get;").execute();
        assertEquals(answers2.size(), 2);
    }

    @Test //Expected result: Both queries should return a single equal match as they trigger the same rule.
    public void reasoningOverRelationHierarchy(){
        QueryBuilder qb = test20.tx().graql().infer(true);
        String queryString = "match (role1: $x, role2: $y) isa relation1; get;";
        String queryString2 = "match (role1: $x, role2: $y) isa sub-relation1; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(answers.size(), 1);
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    @Test //Expected result: Both queries should return a single equal match as they trigger the same rule.
    public void reasoningOverEntityHierarchy(){
        QueryBuilder qb = test21.tx().graql().infer(true);
        String queryString = "match $x isa entity1; get;";
        String queryString2 = "match $x isa sub-entity1; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(answers.size(), 1);
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    @Test //Expected result: Returns db and inferred relations + their inverses and relations with self for all entities
    public void reasoningWithRepeatingRoles(){
        QueryBuilder qb = test22.tx().graql().infer(true);
        String queryString = "match (friend:$x1, friend:$x2) isa knows-trans; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 16);
    }

    @Test //Expected result: The same set of results is always returned
    public void reasoningWithLimitHigherThanNumberOfResults_ReturnsConsistentResults(){
        QueryBuilder qb = test23.tx().graql().infer(true);
        String queryString = "match (friend1:$x1, friend2:$x2) isa knows-trans;limit 60; get;";
        List<ConceptMap> oldAnswers = qb.<GetQuery>parse(queryString).execute();
        for(int i = 0; i < 5 ; i++) {
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(answers.size(), 6);
            assertTrue(answers.containsAll(oldAnswers));
            assertTrue(oldAnswers.containsAll(answers));
        }
    }

    @Test //Expected result: Relations between all entity instances including relation between each instance and itself
    public void reasoningWithEntityTypes() {
        QueryBuilder qb = test24.tx().graql().infer(true);
        QueryBuilder qbm = test24.tx().graql().infer(true);
        String queryString = "match (role1:$x1, role2:$x2) isa relation1; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> answers2 = qbm.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 9);
        assertEquals(answers2.size(), 9);
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    @Test //Expected result: Relations between all entity instances including relation between each instance and itself
    public void reasoningWithEntityTypes_WithNeqProperty() {
        QueryBuilder qb = test24.tx().graql().infer(true);
        String queryString = "match (role1:$x1, role2:$x2) isa relation2; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 6);
    }

    @Test //Expected result: Timeline is correctly recognised via applying resource comparisons in the rule body
    public void reasoningWithResourceValueComparison() {
        QueryBuilder qb = test25.tx().graql().infer(true);
        String queryString = "match (predecessor:$x1, successor:$x2) isa message-succession; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 10);
    }

    //tests if partial substitutions are propagated correctly - atom disjointness may lead to variable loss (bug #15476)
    @Test //Expected result: 2 relations obtained by correctly finding reified relations
    public void reasoningWithReifiedRelations() {
        QueryBuilder qb = test26.tx().graql().infer(true);
        String queryString = "match (role1: $x1, role2: $x2) isa relation2; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 2);

        String queryString2 = "match " +
                "$b isa entity2;" +
                "$b has res1 'value';" +
                "$rel1 has res2 'value1';" +
                "$rel1 (role1: $p, role2: $b) isa relation1;" +
                "$rel2 has res2 'value2';" +
                "$rel2 (role1: $c, role2: $b) isa relation1; get;";
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(answers2.size(), 2);
        Set<Var> vars = Sets.newHashSet(var("b"), var("p"), var("c"), var("rel1"), var("rel2"));
        answers2.forEach(ans -> assertTrue(ans.vars().containsAll(vars)));
    }

    @Test //Expected result: 2 relations obtained by correctly finding reified relations
    public void reasoningWithNeqProperty() {
        QueryBuilder qb = test27.tx().graql().infer(true);
        String queryString = "match (related-state: $s) isa holds; get;";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> exact = qb.<GetQuery>parse("match $s isa state, has name 's2'; get;").execute();
        assertTrue(answers.containsAll(exact));
        assertTrue(exact.containsAll(answers));
    }

    @Test //Expected result: number of answers equal to specified limit (no duplicates produced)
    public void duplicatesNotProducedWhenResolvingNonResolvableConjunctionsWithoutType(){
        QueryBuilder qb = test28.tx().graql().infer(true);
        String queryString = "match " +
                "(role1: $x, role2: $y);" +
                "(role1: $y, role2: $z);" +
                "(role3: $z, role4: $w) isa relation3;" +
                "limit 3; get;";

        assertEquals(qb.<GetQuery>parse(queryString).execute().size(), 3);
    }

    @Test //Expected result: no answers (if types were incorrectly inferred the query would yield answers)
    public void relationTypesAreCorrectlyInferredInConjunction_TypeArePresent(){
        QueryBuilder qb = test28.tx().graql().infer(true);
        String queryString = "match " +
                "(role1: $x, role2: $y) isa relation1;" +
                "(role1: $y, role2: $z) isa relation1;" +
                "(role3: $z, role4: $w) isa relation3; get;";

        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test
    public void relationTypesAreCorrectlyInferredInConjunction_TypesAreAbsent(){
        QueryBuilder qb = test28b.tx().graql().infer(true);
        String queryString = "match " +
                "$a isa entity1;" +
                "($a, $b); $b isa entity3;" +
                "($b, $c);" +
                "get;";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 4);
        answers.forEach(ans -> assertEquals(ans.size(), 3));
    }

    @Test
    public void relationTypesAreCorrectlyInferredInConjunction_TypesAreAbsent_DisconnectedQuery(){
        QueryBuilder qb = test28b.tx().graql().infer(true);

        String pattern = "{$a isa entity1;($a, $b); $b isa entity3;};";
        String pattern2 = "{($c, $d);};";
        String queryString = "match " +
                pattern +
                pattern2 +
                "get;";
        List<ConceptMap> partialAnswers = qb.match(Graql.parser().parsePatterns(pattern)).get().execute();

        //single relation that satisfies the types
        assertEquals(partialAnswers.size(), 1);

        List<ConceptMap> partialAnswers2 = qb.match(Graql.parser().parsePatterns(pattern2)).get().execute();
        //(4 db relations  + 1 inferred + 1 resource) x 2 for variable swap
        assertEquals(partialAnswers2.size(), 12);

        //1 relation satisfying ($a, $b) with types x (4 db relations + 1 inferred + 1 resource) x 2 for var change
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), partialAnswers.size() * partialAnswers2.size());
        answers.forEach(ans -> assertEquals(ans.size(), 4));
    }

    /**
       Should find the possible relation configurations:
         (x, z) - (z, z1) - (z1, z)
                - (z, z2) - (z2, z)
                - (z, y)  - { (y,z) (y, x) }
                - (z, x)  - { res, (x, y), (x, z) }
         */
    @Test
    public void relationTypesAreCorrectlyInferredInConjunction_TypesAreAbsent_WithRelationWithoutAnyBounds(){
        QueryBuilder qb = test28b.tx().graql().infer(true);
        String entryPattern = "{" +
                "$a isa entity1;" +
                "($a, $b);" +
                "};";

        List<ConceptMap> entryAnswers = qb.match(Graql.parser().parsePatterns(entryPattern)).get().execute();
        assertEquals(entryAnswers.size(), 3);

        String partialPattern = "{" +
                "$a isa entity1;" +
                "($a, $b); $b isa entity3;" +
                "($b, $c);" +
                "};";

        List<ConceptMap> partialAnswers = qb.match(Graql.parser().parsePatterns(partialPattern)).get().execute();
        assertEquals(partialAnswers.size(), 4);
        String queryString = "match " +
                partialPattern +
                "($c, $d);" +
                "get;";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 7);
        answers.forEach(ans -> assertEquals(ans.size(), 4));
    }

    @Test //when rule are defined to append new RPs no new relation instances should be created
    public void whenAppendingRolePlayers_noNewRelationsAreCreated(){
        QueryBuilder qb = appendingRPsContext.tx().graql();

        List<ConceptMap> answers = qb.infer(false).<GetQuery>parse("match $r isa relation; get;").execute();
        List<ConceptMap> inferredAnswers = qb.infer(true).<GetQuery>parse("match $r isa relation; get;").execute();
        assertEquals(answers, inferredAnswers);
    }


    @Test
    public void whenQueryingAppendedRelations_rulesAreMatchedCorrectly(){
        QueryBuilder qb = appendingRPsContext.tx().graql().infer(true);
        Set<ConceptMap> variants = Stream.of(
                Iterables.getOnlyElement(qb.<GetQuery>parse("match $r (someRole: $x, anotherRole: $y, anotherRole: $z, inferredRole: $z); $y != $z;get;").execute()),
                Iterables.getOnlyElement(qb.<GetQuery>parse("match $r (someRole: $x, inferredRole: $z ); $x has resource 'value'; get;").execute()),
                Iterables.getOnlyElement(qb.<GetQuery>parse("match $r (someRole: $x, yetAnotherRole: $y, andYetAnotherRole: $y, inferredRole: $z); get;").execute()),
                Iterables.getOnlyElement(qb.<GetQuery>parse("match $r (anotherRole: $x, andYetAnotherRole: $y); get;").execute())
        )
                .map(ans-> ans.project(Sets.newHashSet(var("r"))))
                .collect(Collectors.toSet());

        List<ConceptMap> answers = qb.<GetQuery>parse("match $r isa relation; get;").execute();
        assertCollectionsEqual(variants, answers);
    }

    @Test
    public void whenRuleContainsRelationRequiringAppend_bodyIsRewrittenCorrectly(){
        QueryBuilder qb = appendingRPsContext.tx().graql().infer(true);

        List<ConceptMap> answers = qb.<GetQuery>parse("match (inferredRole: $x, inferredRole: $y, inferredRole: $z) isa derivedRelation; get;").execute();
        assertEquals(2, answers.size());
    }

    @Test //tests a query containing a neq predicate bound to a recursive relation
    public void recursiveRelationWithNeqPredicate(){
        QueryBuilder qb = test29.tx().graql().infer(true);
        String baseQueryString = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "$x != $y;";
        String queryString = baseQueryString + "$y has name 'c'; get;";

        List<ConceptMap> baseAnswers = qb.<GetQuery>parse(baseQueryString + "get;").execute();
        assertEquals(baseAnswers.size(), 6);
        baseAnswers.forEach(ans -> {
            assertEquals(ans.size(), 2);
            assertNotEquals(ans.get("x"), ans.get("y"));
        });

        String explicitString = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "$y has name 'c';" +
                "{$x has name 'a';} or {$x has name 'b';}; get;";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> answers2 = qb.<GetQuery>parse(explicitString).execute();
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    /**
     * Tests a scenario in which the neq predicate binds free variables of two recursive equivalent relations.
     * Corresponds to the following pattern:
     *
     *                     x
     *                   /    \
     *                 /        \
     *               v           v
     *              y     !=      z
     */
    @Test
    public void recursiveRelationsWithSharedNeqPredicate_relationsAreEquivalent(){
        QueryBuilder qb = test29.tx().graql().infer(true);
        String baseQueryString = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "(role1: $x, role2: $z) isa binary-base;" +
                "$y != $z;";

        List<ConceptMap> baseAnswers = qb.<GetQuery>parse(baseQueryString + "get;").execute();
        assertEquals(baseAnswers.size(), 18);
        baseAnswers.forEach(ans -> {
            assertEquals(ans.size(), 3);
            assertNotEquals(ans.get("y"), ans.get("z"));
        });

        String queryString = baseQueryString + "$x has name 'a';";
        String explicitString = "match " +
                "$x has name 'a';" +
                "{$y has name 'a';$z has name 'b';} or " +
                "{$y has name 'a';$z has name 'c';} or " +
                "{$y has name 'b';$z has name 'a';} or" +
                "{$y has name 'b';$z has name 'c';} or " +
                "{$y has name 'c';$z has name 'a';} or " +
                "{$y has name 'c';$z has name 'b';};";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString + "get;").execute();
        List<ConceptMap> answers2 = qb.infer(false).<GetQuery>parse(explicitString + "get;").execute();
        assertTrue(baseAnswers.containsAll(answers));
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    /**
     * Tests a scenario in which the neq predicate prevents loops by binding free variables
     * of two recursive non-equivalent relations. Corresponds to the following pattern:
     *
     *                     y
     *                    ^  \
     *                  /      \
     *                /          v
     *              x     !=      z
     */
    @Test
    public void multipleRecursiveRelationsWithSharedNeqPredicate_neqPredicatePreventsLoops(){
        QueryBuilder qb = test29.tx().graql().infer(true);
        String baseQueryString = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "(role1: $y, role2: $z) isa binary-base;" +
                "$x != $z;";

        List<ConceptMap> baseAnswers = qb.<GetQuery>parse(baseQueryString + "get;").execute();
        assertEquals(baseAnswers.size(), 18);
        baseAnswers.forEach(ans -> {
            assertEquals(ans.size(), 3);
            assertNotEquals(ans.get("x"), ans.get("z"));
        });

        String queryString = baseQueryString + "$x has name 'a';";

        String explicitString = "match " +
                "$x has name 'a';" +
                "{$y has name 'a';$z has name 'b';} or " +
                "{$y has name 'a';$z has name 'c';} or " +
                "{$y has name 'b';$z has name 'c';} or " +
                "{$y has name 'b';$z has name 'b';} or " +
                "{$y has name 'c';$z has name 'c';} or " +
                "{$y has name 'c';$z has name 'b';};";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString + "get;").execute();
        List<ConceptMap> answers2 = qb.infer(false).<GetQuery>parse(explicitString + "get;").execute();
        assertTrue(answers.containsAll(answers2));
        assertTrue(answers2.containsAll(answers));
    }

    /**
     * Tests a scenario in which the multiple neq predicates are present but bind at most single var in a relation.
     * Corresponds to the following pattern:
     *
     *              y       !=      z1
     *               ^              ^
     *                 \           /
     *                   \       /
     *                      x[a]
     *                   /      \
     *                 /          \
     *                v            v
     *              y2     !=      z2
     */
    @Test
    public void multipleRecursiveRelationsWithMultipleSharedNeqPredicates_symmetricPattern(){
        QueryBuilder qb = test29.tx().graql().infer(true);
        String baseQueryString = "match " +
                "(role1: $x, role2: $y1) isa binary-base;" +
                "(role1: $x, role2: $z1) isa binary-base;" +
                "(role1: $x, role2: $y2) isa binary-base;" +
                "(role1: $x, role2: $z2) isa binary-base;" +

                "$y1 != $z1;" +
                "$y2 != $z2;";

        List<ConceptMap> baseAnswers = qb.<GetQuery>parse(baseQueryString + "get;").execute();
        assertEquals(baseAnswers.size(), 108);
        baseAnswers.forEach(ans -> {
            assertEquals(ans.size(), 5);
            assertNotEquals(ans.get("y1"), ans.get("z1"));
            assertNotEquals(ans.get("y2"), ans.get("z2"));
        });

        String queryString = baseQueryString + "$x has name 'a';";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString + "get;").execute();
        assertEquals(answers.size(), 36);
        answers.forEach(ans -> {
            assertEquals(ans.size(), 5);
            assertNotEquals(ans.get("y1"), ans.get("z1"));
            assertNotEquals(ans.get("y2"), ans.get("z2"));
        });
    }

    /**
     * Tests a scenario in which a single relation has both variables bound with two different neq predicates.
     * Corresponds to the following pattern:
     *
     *                  x[a]  - != - >  z1
     *                  |
     *                  |
     *                  v
     *                  y     - != - >  z2
     */
    @Test
    public void multipleRecursiveRelationsWithMultipleSharedNeqPredicates(){
        QueryBuilder qb = test29.tx().graql().infer(true);
        String baseQueryString = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "$x != $z1;" +
                "$y != $z2;" +
                "(role1: $x, role2: $z1) isa binary-base;" +
                "(role1: $y, role2: $z2) isa binary-base;";

        List<ConceptMap> baseAnswers = qb.<GetQuery>parse(baseQueryString + "get;").execute();
        assertEquals(baseAnswers.size(), 36);
        baseAnswers.forEach(ans -> {
            assertEquals(ans.size(), 4);
            assertNotEquals(ans.get("x"), ans.get("z1"));
            assertNotEquals(ans.get("y"), ans.get("z2"));
        });

        String queryString = baseQueryString + "$x has name 'a';";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString + "get;").execute();
        assertEquals(answers.size(), 12);
        answers.forEach(ans -> {
            assertEquals(ans.size(), 4);
            assertNotEquals(ans.get("x"), ans.get("z1"));
            assertNotEquals(ans.get("y"), ans.get("z2"));
        });
    }

    @Test //tests whether shared resources are recognised correctly
    public void inferrableRelationWithRolePlayersSharingResource(){
        QueryBuilder qb = test29.tx().graql().infer(true);
        String queryString = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "$x has name $n;" +
                "$y has name $n;" +
                "get;";

        String queryString2 = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "$x has name $n;" +
                "$y has name $n;" +
                "$n == 'a';" +
                "get;";

        String queryString3 = "match " +
                "(role1: $x, role2: $y) isa binary-base;" +
                "$x has name 'a';" +
                "$y has name 'a';" +
                "get;";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
        List<ConceptMap> answers3 = qb.<GetQuery>parse(queryString3).execute();

        assertEquals(answers.size(), 3);
        answers.forEach(ans -> {
            assertEquals(ans.size(), 3);
            assertEquals(ans.get("x"), ans.get("y"));
        });

        assertEquals(answers2.size(), 1);

        assertEquals(answers3.size(), 1);
        answers2.stream()
                .map(a -> a.project(Sets.newHashSet(var("x"), var("y"))))
                .forEach(a -> assertTrue(answers3.contains(a)));
    }

    @Test
    public void ternaryRelationsRequiryingDifferentMultiunifiers(){
        QueryBuilder qb = test29.tx().graql().infer(true);

        String queryString = "match " +
                "(role1: $a, role2: $b, role3: $c) isa ternary-base;" +
                "get;";

        String queryString2 = "match " +
                "(role: $a, role2: $b, role: $c) isa ternary-base;" +
                "$b has name 'b';" +
                "get;";

        String queryString3 = "match " +
                "($r: $a) isa ternary-base;" +
                "get;";

        String queryString4 = "match " +
                "($r: $b) isa ternary-base;" +
                "$b has name 'b';" +
                "get;";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 27);

        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
        assertEquals(answers2.size(), 9);

        List<ConceptMap> answers3 = qb.<GetQuery>parse(queryString3).execute();
        assertEquals(answers3.size(), 12);

        List<ConceptMap> answers4 = qb.<GetQuery>parse(queryString4).execute();
        assertEquals(answers4.size(), 4);
    }

    @Test
    public void binaryRelationWithDifferentVariantsOfVariableRoles(){
        QueryBuilder qb = test29.tx().graql().infer(true);

        //9 binary-base instances with {role, role2} = 2 roles for r2 -> 18 answers
        String queryString = "match " +
                "(role1: $a, $r2: $b) isa binary-base;" +
                "get;";

        String equivalentQueryString = "match " +
                "($r1: $a, $r2: $b) isa binary-base;" +
                "$r1 label 'role1';" +
                "get $a, $b, $r2;";

        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        List<ConceptMap> equivalentAnswers = qb.<GetQuery>parse(equivalentQueryString).execute();
        assertEquals(answers.size(), 18);
        assertTrue(CollectionUtils.isEqualCollection(answers, equivalentAnswers));

        //9 binary-base instances with {role, role1, role2} = 3 roles for r2 -> 27 answers
        String queryString2 = "match " +
                "(role: $a, $r2: $b) isa binary-base;" +
                "get;";

        String equivalentQueryString2 = "match " +
                "($r1: $a, $r2: $b) isa binary-base;" +
                "$r1 label 'role';" +
                "get $a, $b, $r2;";

        List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
        List<ConceptMap> equivalentAnswers2 = qb.<GetQuery>parse(equivalentQueryString2).execute();
        assertEquals(answers2.size(), 27);
        assertTrue(CollectionUtils.isEqualCollection(answers2, equivalentAnswers2));

        //role variables bound hence should return original 9 instances
        String queryString3 = "match " +
                "($r1: $a, $r2: $b) isa binary-base;" +
                "$r1 label 'role';" +
                "$r2 label 'role2';" +
                "get $a, $b;";

        String equivalentQueryString3 = "match " +
                "(role1: $a, role2: $b) isa binary-base;" +
                "get;";

        List<ConceptMap> answers3 = qb.<GetQuery>parse(queryString3).execute();
        List<ConceptMap> equivalentAnswers3 = qb.<GetQuery>parse(equivalentQueryString3).execute();
        assertEquals(answers3.size(), 9);
        assertTrue(CollectionUtils.isEqualCollection(answers3, equivalentAnswers3));

        //9 relation instances with 7 possible permutations for each - 63 answers
        String queryString4 = "match " +
                "($r1: $a, $r2: $b) isa binary-base;" +
                "get;";

        List<ConceptMap> answers4 = qb.<GetQuery>parse(queryString4).execute();
        assertEquals(answers4.size(), 63);
    }

    @Test
    public void binaryRelationWithVariableRoles_basicSet(){
        final int conceptDOF = 2;
        ternaryNaryRelationWithVariableRoles("binary", conceptDOF);
    }

    @Test
    public void binaryRelationWithVariableRoles_extendedSet(){
        final int conceptDOF = 3;
        ternaryNaryRelationWithVariableRoles("binary-base", conceptDOF);
    }

    @Test
    public void ternaryRelationWithVariableRoles_basicSet(){
        /*
        As each vertex is a starting point for {a, b, c} x {a, b c} = 9 relations, starting with a we have:

        (r1: a, r2: a, r3: a), (r1: a, r2: a, r3: b), (r1: a, r2: a, r3: c)
        (r1: a, r2: b, r3: a), (r1: a, r2: b, r3: b), (r1: a, r2: b, r3: c)
        (r1: a, r2: c, r3: a), (r1: a, r2: c, r3: b), (r1: a. r2: c, r3: c)

        If we generify two roles each of these produces 7 answers, taking (r1: a, r2: b, r3:c) we have:

        (a, r2: b, r3: c)
        (a, r: b, r3: c)
        (a, r2: b, r: c)
        (a, r3: c, r2: b)
        (a, r3: c, r: b)
        (a, r: c, r2: b)
        (a, r: b, r: c)

        plus
        (a, r: c, r: b) but this one is counted in (r1: a, r2: c, r3:b)
        hence 7 answers per single relation.
        */
        final int conceptDOF = 2;
        ternaryNaryRelationWithVariableRoles("ternary", conceptDOF);
    }

    @Test
    public void ternaryRelationWithVariableRoles_extendedSet(){
        final int conceptDOF = 3;
        ternaryNaryRelationWithVariableRoles("ternary-base", conceptDOF);
    }

    @Test
    public void quaternaryRelationWithVariableRoles_basicSet(){
        final int conceptDOF = 2;
        ternaryNaryRelationWithVariableRoles("quaternary", conceptDOF);
    }

    @Test
    public void quaternaryRelationWithVariableRoles2_extendedSet(){
        final int conceptDOF = 3;
        ternaryNaryRelationWithVariableRoles("quaternary-base", conceptDOF);
    }

    private void ternaryNaryRelationWithVariableRoles(String label, int conceptDOF){
        GraknTx graph = test29.tx();
        QueryBuilder qb = graph.graql().infer(true);
        final int arity = (int) graph.getRelationshipType(label).roles().count();

        VarPattern resourcePattern = var("a1").has("name", "a");

        //This query generalises all roles but the first one.
        VarPattern pattern = var().rel("role1", "a1");
        for(int i = 2; i <= arity ; i++) pattern = pattern.rel(var("r" + i), "a" + i);
        pattern = pattern.isa(label);

        List<ConceptMap> answers = qb.match(pattern.and(resourcePattern)).get().execute();
        assertEquals(answers.size(), answerCombinations(arity-1, conceptDOF));

        //We get extra conceptDOF degrees of freedom by removing the resource constraint on $a1 and the set is symmetric.
        List<ConceptMap> answers2 = qb.match(pattern).get().execute();
        assertEquals(answers2.size(), answerCombinations(arity-1, conceptDOF) * conceptDOF);


        //The general case of mapping all available Rps
        VarPattern generalPattern = var();
        for(int i = 1; i <= arity ; i++) generalPattern = generalPattern.rel(var("r" + i), "a" + i);
        generalPattern = generalPattern.isa(label);

        List<ConceptMap> answers3 = qb.match(generalPattern).get().execute();
        assertEquals(answers3.size(), answerCombinations(arity, conceptDOF));
    }

    /**
     *Each role player variable can be mapped to either of the conceptDOF concepts and these can repeat.
     *Each role variable can be mapped to either of RPs roles and only meta roles can repeat.

     *For the case of conceptDOF = 3, roleDOF = 3.
     *We start by considering the number of meta roles we allow.
     *If we consider only non-meta roles, considering each relation player we get:
     *C^3_0 x 3.3 x 3.2 x 3 = 162 combinations
     *
     *If we consider single metarole - C^3_1 = 3 possibilities of assigning them:
     *C^3_1 x 3.3 x 3.2 x 3 = 486 combinations
     *
     *Two metaroles - again C^3_2 = 3 possibilities of assigning them:
     *C^3_2 x 3.3 x 3   x 3 = 243 combinations
     *
     *Three metaroles, C^3_3 = 1 possiblity of assignment:
     *C^3_3 x 3   x 3   x 3 = 81 combinations
     *
     *-> Total = 918 different answers
     *In general, for i allowed meta roles we have:
     *C^{RP}_i PRODUCT_{j = RP-i}{ (conceptDOF)x(roleDOF-j) } x PRODUCT_i{ conceptDOF} } answers.
     *
     *So total number of answers is:
     *SUM_i{ C^{RP}_i PRODUCT_{j = RP-i}{ (conceptDOF)x(roleDOF-j) } x PRODUCT_i{ conceptDOF} }
     *
     * @param RPS number of relation players available
     * @param conceptDOF number of concept degrees of freedom
     * @return number of answer combinations
     */
    private int answerCombinations(int RPS, int conceptDOF) {
        int answers = 0;
        //i is the number of meta roles
        for (int i = 0; i <= RPS; i++) {
            int RPProduct = 1;
            //rps with non-meta roles
            for (int j = 0; j < RPS - i; j++) RPProduct *= conceptDOF * (RPS - j);
            //rps with meta roles
            for (int k = 0; k < i; k++) RPProduct *= conceptDOF;
            answers += CombinatoricsUtils.binomialCoefficient(RPS, i) * RPProduct;
        }
        return answers;
    }

    @Test //tests scenario where rules define mutually recursive relation and resource and we query for an attributed type corresponding to the relation
    public void mutuallyRecursiveRelationAndResource_queryForAttributedType(){
        QueryBuilder qb = test30.tx().graql().infer(true);

        String queryString = "match $p isa pair, has name 'ff'; get;";
        List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
        assertEquals(answers.size(), 16);
    }
}
