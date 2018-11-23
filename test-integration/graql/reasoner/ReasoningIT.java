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

package grakn.core.graql.reasoner;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.QueryBuilder;
import grakn.core.graql.query.Var;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.ClassRule;
import org.junit.Test;

import static grakn.core.graql.query.Graql.var;
import static grakn.core.graql.internal.Schema.ImplicitType.HAS;
import static grakn.core.graql.internal.Schema.ImplicitType.HAS_OWNER;
import static grakn.core.graql.internal.Schema.ImplicitType.HAS_VALUE;
import static grakn.core.util.GraqlTestUtil.assertCollectionsEqual;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Suite of tests checking different meanders and aspects of reasoning - full reasoning cycle is being tested.
 */
@SuppressWarnings("CheckReturnValue")
public class ReasoningIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static String resourcePath = "test-integration/graql/reasoner/stubs/";

    //The tests validate the correctness of the rule reasoning implementation w.r.t. the intended semantics of rules.
    //The ignored tests reveal some bugs in the reasoning algorithm, as they don't return the expected results,
    //as specified in the respective comments below.

    @Test
    public void attributeOwnerResultsAreConsistentBetweenDifferentAccessPoints(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "resourceDirectionality.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(false);

                List<ConceptMap> answers = qb.<GetQuery>parse("match $x isa specific-indicator;get;").execute();

                Concept indicator = answers.iterator().next().get("x");

                GetQuery attributeQuery = qb.parse("match $x has attribute $r; $x id " + indicator.id().getValue() + ";get;");
                GetQuery attributeRelationQuery = qb.parse("match (@has-attribute-owner: $x, $r) isa @has-attribute; $x id " + indicator.id().getValue() + ";get;");

                Set<Attribute<Object>> attributes = attributeQuery.stream().map(ans -> ans.get("r")).map(Concept::asAttribute).collect(toSet());
                Set<Attribute<Object>> attributesFromImplicitRelation = attributeRelationQuery.stream().map(ans -> ans.get("r")).map(Concept::asAttribute).collect(toSet());
                Set<Attribute<?>> attributesFromAPI = indicator.asThing().attributes().collect(Collectors.toSet());

                assertThat(attributes, empty());
                assertEquals(attributes, attributesFromAPI);
                assertEquals(attributes, attributesFromImplicitRelation);

                qb.parse("match $rmn isa model-name 'someName', has specific-indicator 'someIndicator' via $a; insert $a has indicator-name 'someIndicatorName';").execute();

                Set<Attribute<Object>> newAttributes = attributeQuery.stream().map(ans -> ans.get("r")).map(Concept::asAttribute).collect(toSet());
                Set<Attribute<Object>> newAttributesFromImplicitRelation = attributeRelationQuery.stream().map(ans -> ans.get("r")).map(Concept::asAttribute).collect(toSet());
                Set<Attribute<?>> newAttributesFromAPI = indicator.asThing().attributes().collect(Collectors.toSet());

                assertThat(newAttributes, empty());
                assertEquals(newAttributes, newAttributesFromAPI);
                assertEquals(newAttributes, newAttributesFromImplicitRelation);
            }
        }
    }

    @Test
    public void resourceHierarchiesAreRespected() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "resourceHierarchy.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
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
        }
    }

    @Test
    public void resourceOwnershipNotPropagatedWithinRelation() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "resourceOwnership.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
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

                tx.getMetaEntityType().instances().forEach(entity -> assertThat(entity.attributes().collect(toSet()), empty()));

                AttributeType<String> name = tx.getAttributeType("name");
                Set<Attribute<String>> instances = name.instances().collect(Collectors.toSet());
                instances.forEach(attribute -> assertThat(attribute.owners().collect(toSet()), empty()));

                assertThat(answers, empty());
                assertCollectionsEqual(implicitAnswers, answers);
            }
        }
    }

    @Test //Expected result: Both queries should return a non-empty result, with $x/$y mapped to a unique entity.
    public void unificationOfReflexiveRelations() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "reflexiveRelation.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);
                String queryString = "match (role1:$x, role2:$x) isa relation1; get;";
                String queryString2 = "match (role1:$x, role2:$y) isa relation1; get;";
                List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
                List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();

                assertEquals(1, answers.size());
                answers.forEach(x -> assertEquals(1, x.size()));

                assertEquals(4, answers2.size());

                assertNotEquals(0, answers.size() * answers2.size());

                answers2.forEach(x -> assertEquals(2, x.size()));
            }
        }
    }

    @Test //Expected result: Both queries should return a non-empty result, with $x/$y mapped to a unique entity.
    public void unificationOfReflexiveSymmetricRelations() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "reflexiveSymmetricRelation.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);
                String queryString = "match (symmetricRole: $x, symmetricRole: $x) isa symmetricRelation; get;";
                String queryString2 = "match (symmetricRole: $x, symmetricRole: $y) isa symmetricRelation; get;";
                List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
                List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();

                assertEquals(2, answers.size());
                assertEquals(8, answers2.size());
                assertNotEquals(0, answers.size() * answers2.size());
                answers.forEach(x -> assertEquals(1, x.size()));
                answers2.forEach(x -> assertEquals(2, x.size()));
            }
        }
    }

    @Test //Expected result: The query should return 10 unique matches (no duplicates).
    public void distinctLimitedAnswersOfInfinitelyGeneratingRule() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet7.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder iqb = tx.graql().infer(true);
                QueryBuilder qb = tx.graql().infer(false);
                String queryString = "match $x isa relation1; limit 10; get;";
                List<ConceptMap> answers = iqb.<GetQuery>parse(queryString).execute();
                assertEquals(10, answers.size());
                assertEquals(qb.<GetQuery>parse(queryString).execute().size(), answers.size());
            }
        }
    }

    @Test //Expected result: The query should not return any matches (or possibly return a single match with $x=$y)
    public void roleUnificationWithRepeatingRoleTypes() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet9.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);
                String doubleRpQuery = "match (role1:$x, role1:$y) isa relation2; get;";
                List<ConceptMap> answers = qb.<GetQuery>parse(doubleRpQuery).execute();
                assertThat(answers, empty());

                String singleRpQuery = "match (role1:$x) isa relation2; get;";
                List<ConceptMap> answers2 = qb.<GetQuery>parse(singleRpQuery).execute();
                assertEquals(1, answers2.size());
            }
        }
    }

    /**
     * recursive relation having same type for different role players
     * tests for handling recursivity and equivalence of queries and relations
     */
    @Test //Expected result: The query should return a unique match
    public void transRelationWithEntityGuardsAtBothEnds() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet10.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);
                String queryString = "match (role1: $x, role2: $y) isa relation2; get;";
                List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
                assertEquals(1, answers.size());
            }
        }
    }

    @Test //Expected result: The query should return a unique match
    public void transRelationWithRelationGuardsAtBothEnds() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet11.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);
                String queryString = "match (role1:$x, role2:$y) isa relation3; get;";
                assertEquals(1, qb.<GetQuery>parse(queryString).execute().size());
            }
        }
    }

    @Test //Expected result: The query should return two unique matches
    public void circularRuleDependencies() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet12.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);
                String queryString = "match (role1:$x, role2:$y) isa relation3; get;";
                List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
                assertEquals(2, answers.size());
            }
        }
    }

    @Test
    public void resourcesAsRolePlayers() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "resourcesAsRolePlayers.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);

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

                assertEquals(2, answers.size());
                assertEquals(1, answers2.size());
                assertEquals(1, answers3.size());
                assertEquals(1, answers4.size());
                assertEquals(answers.size() + answers2.size() + answers3.size() + answers4.size(), answers5.size());
                assertEquals(answers5.size() - answers4.size(), answers6.size());
            }
        }

    }

    @Test
    public void resourcesAsRolePlayers_vpPropagationTest() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "resourcesAsRolePlayers.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);

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

                assertEquals(3, answers.size());
                assertEquals(3, answers2.size());
                assertEquals(3, answers3.size());
                assertEquals(3, answers4.size());
                assertEquals(answers5.size(), answers.size() + answers2.size() + answers3.size() + answers4.size());
                assertEquals(answers6.size(), answers5.size() - answers4.size());
            }
        }
    }

    @Test //Expected result: Returns db and inferred relations + their inverses and relations with self for all entities
    public void reasoningWithRepeatingRoles(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet22.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);
                String queryString = "match (friend:$x1, friend:$x2) isa knows-trans; get;";
                List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
                assertEquals(16, answers.size());
            }
        }
    }

    @Test //Expected result: The same set of results is always returned
    public void reasoningWithLimitHigherThanNumberOfResults_ReturnsConsistentResults(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet23.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);
                String queryString = "match (friend1:$x1, friend2:$x2) isa knows-trans;limit 60; get;";
                List<ConceptMap> oldAnswers = qb.<GetQuery>parse(queryString).execute();
                for (int i = 0; i < 5; i++) {
                    List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
                    assertEquals(6, answers.size());
                    assertCollectionsEqual(oldAnswers, answers);
                }
            }
        }
    }

    @Test //Expected result: Relations between all entity instances including relation between each instance and itself
    public void reasoningWithEntityTypes() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet24.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);
                String reflexiveQuery = "match (role1:$x1, role2:$x2) isa reflexiveRelation; get;";
                List<ConceptMap> reflexive = qb.<GetQuery>parse(reflexiveQuery).execute();
                assertEquals(9, reflexive.size());

                String uniquePairQuery = "match (role1:$x1, role2:$x2) isa uniquePairRelation; get;";
                List<ConceptMap> uniquePairs = qb.<GetQuery>parse(uniquePairQuery).execute();
                assertEquals(6, uniquePairs.size());
            }
        }
    }

    @Test //Expected result: Timeline is correctly recognised via applying resource comparisons in the rule body
    public void reasoningWithResourceValueComparison() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet25.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);
                String queryString = "match (predecessor:$x1, successor:$x2) isa message-succession; get;";
                List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
                assertEquals(10, answers.size());
            }
        }
    }

    //tests if partial substitutions are propagated correctly - atom disjointness may lead to variable loss (bug #15476)
    @Test //Expected result: 2 relations obtained by correctly finding reified relations
    public void reasoningWithReifiedRelations() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet26.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);
                String queryString = "match (role1: $x1, role2: $x2) isa relation2; get;";
                List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
                assertEquals(2, answers.size());

                String queryString2 = "match " +
                        "$b isa entity2;" +
                        "$b has res1 'value';" +
                        "$rel1 has res2 'value1';" +
                        "$rel1 (role1: $p, role2: $b) isa relation1;" +
                        "$rel2 has res2 'value2';" +
                        "$rel2 (role1: $c, role2: $b) isa relation1; get;";
                List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
                assertEquals(2, answers2.size());
                Set<Var> vars = Sets.newHashSet(var("b"), var("p"), var("c"), var("rel1"), var("rel2"));
                answers2.forEach(ans -> assertTrue(ans.vars().containsAll(vars)));
            }
        }
    }

    @Test //Expected result: number of answers equal to specified limit (no duplicates produced)
    public void whenReasoningWithRelationConjunctions_duplicatesNotProducesAndTypesInferredCorrectly(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet28.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);
                String queryWithTypes = "match " +
                        "(role1: $x, role2: $y);" +
                        "(role1: $y, role2: $z);" +
                        "(role3: $z, role4: $w) isa relation3;" +
                        "limit 3; get;";

                assertEquals(3, qb.<GetQuery>parse(queryWithTypes).execute().size());

                String typeAmbiguousQuery = "match " +
                        "(role1: $x, role2: $y) isa relation1;" +
                        "(role1: $y, role2: $z) isa relation1;" +
                        "(role3: $z, role4: $w) isa relation3; get;";

                assertThat(qb.<GetQuery>parse(typeAmbiguousQuery).execute(), empty());
            }
        }
    }

    @Test
    public void relationTypesAreCorrectlyInferredInConjunction_TypesAreAbsent(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet28b.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);
                String queryString = "match " +
                        "$a isa entity1;" +
                        "($a, $b); $b isa entity3;" +
                        "($b, $c);" +
                        "get;";

                List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
                assertEquals(4, answers.size());
                answers.forEach(ans -> assertEquals(3, ans.size()));
            }
        }
    }

    @Test
    public void relationTypesAreCorrectlyInferredInConjunction_TypesAreAbsent_DisconnectedQuery(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet28b.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);

                String pattern = "{$a isa entity1;($a, $b); $b isa entity3;};";
                String pattern2 = "{($c, $d);};";
                String queryString = "match " +
                        pattern +
                        pattern2 +
                        "get;";
                List<ConceptMap> partialAnswers = qb.match(Graql.parser().parsePatterns(pattern)).get().execute();

                //single relation that satisfies the types
                assertEquals(1, partialAnswers.size());

                List<ConceptMap> partialAnswers2 = qb.match(Graql.parser().parsePatterns(pattern2)).get().execute();
                //(4 db relations  + 1 inferred + 1 resource) x 2 for variable swap
                assertEquals(12, partialAnswers2.size());

                //1 relation satisfying ($a, $b) with types x (4 db relations + 1 inferred + 1 resource) x 2 for var change
                List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
                assertEquals(answers.size(), partialAnswers.size() * partialAnswers2.size());
                answers.forEach(ans -> assertEquals(4, ans.size()));
            }
        }
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
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet28b.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);
                String entryPattern = "{" +
                        "$a isa entity1;" +
                        "($a, $b);" +
                        "};";

                List<ConceptMap> entryAnswers = qb.match(Graql.parser().parsePatterns(entryPattern)).get().execute();
                assertEquals(3, entryAnswers.size());

                String partialPattern = "{" +
                        "$a isa entity1;" +
                        "($a, $b); $b isa entity3;" +
                        "($b, $c);" +
                        "};";

                List<ConceptMap> partialAnswers = qb.match(Graql.parser().parsePatterns(partialPattern)).get().execute();
                assertEquals(4, partialAnswers.size());
                String queryString = "match " +
                        partialPattern +
                        "($c, $d);" +
                        "get;";

                List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
                assertEquals(7, answers.size());
                answers.forEach(ans -> assertEquals(4, ans.size()));
            }
        }
    }

    @Test //when rule are defined to append new RPs no new relation instances should be created
    public void whenAppendingRolePlayers_noNewRelationsAreCreated(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "appendingRPs.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(false);
                QueryBuilder iqb = tx.graql().infer(true);

                List<ConceptMap> persistedRelations = qb.<GetQuery>parse("match $r isa relation; get;").execute();
                List<ConceptMap> inferredRelations = iqb.<GetQuery>parse("match $r isa relation; get;").execute();
                assertEquals("New relations were created!", persistedRelations, inferredRelations);

                Set<ConceptMap> variants = Stream.of(
                        Iterables.getOnlyElement(qb.<GetQuery>parse("match $r (someRole: $x, anotherRole: $y, anotherRole: $z, inferredRole: $z); $y != $z;get;").execute()),
                        Iterables.getOnlyElement(qb.<GetQuery>parse("match $r (someRole: $x, inferredRole: $z ); $x has resource 'value'; get;").execute()),
                        Iterables.getOnlyElement(qb.<GetQuery>parse("match $r (someRole: $x, yetAnotherRole: $y, andYetAnotherRole: $y, inferredRole: $z); get;").execute()),
                        Iterables.getOnlyElement(qb.<GetQuery>parse("match $r (anotherRole: $x, andYetAnotherRole: $y); get;").execute())
                )
                        .map(ans -> ans.project(Sets.newHashSet(var("r"))))
                        .collect(Collectors.toSet());

                assertCollectionsEqual("Rules are not matched correctly!", variants, inferredRelations);

                List<ConceptMap> derivedRPTriples = iqb.<GetQuery>parse("match (inferredRole: $x, inferredRole: $y, inferredRole: $z) isa derivedRelation; get;").execute();
                assertEquals("Rule body is not rewritten correctly!", 2, derivedRPTriples.size());
            }
        }
    }

    @Test //tests whether shared resources are recognised correctly
    public void inferrableRelationWithRolePlayersSharingResource(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet29.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);
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

                assertEquals(3, answers.size());
                answers.forEach(ans -> {
                    assertEquals(3, ans.size());
                    assertEquals(ans.get("x"), ans.get("y"));
                });

                assertEquals(1, answers2.size());
                assertEquals(1, answers3.size());
                answers2.stream()
                        .map(a -> a.project(Sets.newHashSet(var("x"), var("y"))))
                        .forEach(a -> assertTrue(answers3.contains(a)));
            }
        }
    }

    @Test
    public void ternaryRelationsRequiryingDifferentMultiunifiers(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet29.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);

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
                assertEquals(27, answers.size());

                List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
                assertEquals(9, answers2.size());

                List<ConceptMap> answers3 = qb.<GetQuery>parse(queryString3).execute();
                assertEquals(12, answers3.size());

                List<ConceptMap> answers4 = qb.<GetQuery>parse(queryString4).execute();
                assertEquals(4, answers4.size());
            }
        }
    }

    @Test
    //tests scenario where rules define mutually recursive relation and resource and we query for an attributed type corresponding to the relation
    public void mutuallyRecursiveRelationAndResource_queryForAttributedType(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet30.gql", session);
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                QueryBuilder qb = tx.graql().infer(true);

                String queryString = "match $p isa pair, has name 'ff'; get;";
                List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
                assertEquals(16, answers.size());
            }
        }
    }
}
