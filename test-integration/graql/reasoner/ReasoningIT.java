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
import grakn.core.graql.GetQuery;
import grakn.core.graql.Graql;
import grakn.core.graql.Query;
import grakn.core.graql.QueryBuilder;
import grakn.core.graql.Var;
import grakn.core.graql.VarPattern;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionImpl;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import static grakn.core.graql.Graql.label;
import static grakn.core.graql.Graql.var;
import static grakn.core.graql.internal.Schema.ImplicitType.HAS;
import static grakn.core.graql.internal.Schema.ImplicitType.HAS_OWNER;
import static grakn.core.graql.internal.Schema.ImplicitType.HAS_VALUE;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.collections.CollectionUtils.isEqualCollection;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

    private static SessionImpl reflexiveRelationSession;
    private static SessionImpl reflexiveSymmetricRelationSession;

    private static SessionImpl typeDerivationSession;
    private static SessionImpl typeDerivationWithDirectSession;
    private static SessionImpl typeDerivationRelationsWithDirectSession;
    private static SessionImpl typeDerivationFromAttributeSession;
    private static SessionImpl typeDerivationFromRelationsSession;

    private static SessionImpl freshEntityDerivationSession;
    private static SessionImpl freshEntityDerivationFromRelationsSession;
    private static SessionImpl freshRelationDerivationSession;

    private static SessionImpl appendingRPsContextSession;
    private static SessionImpl resourceAttachmentSession;
    private static SessionImpl resourcesAsRolePlayersSession;

    private static SessionImpl test7Session;
    private static SessionImpl test8Session;
    private static SessionImpl test9Session;
    private static SessionImpl test10Session;
    private static SessionImpl test11Session;
    private static SessionImpl test12Session;
    private static SessionImpl test13Session;
    private static SessionImpl test19Session;
    private static SessionImpl test19recursiveSession;
    private static SessionImpl test20Session;
    private static SessionImpl test21Session;
    private static SessionImpl test22Session;
    private static SessionImpl test23Session;
    private static SessionImpl test24Session;
    private static SessionImpl test25Session;
    private static SessionImpl test26Session;
    private static SessionImpl test27Session;
    private static SessionImpl test28Session;
    private static SessionImpl test28bSession;
    private static SessionImpl test29Session;
    private static SessionImpl test30Session;

    private static SessionImpl resourceOwnershipSession;
    private static SessionImpl resourceHierarchySession;
    private static SessionImpl resourceDirectionalitySession;

    @BeforeClass
    public static void loadContext() {
        reflexiveRelationSession = server.sessionWithNewKeyspace();
        loadFromFile("reflexiveRelation.gql", reflexiveRelationSession);
        reflexiveSymmetricRelationSession = server.sessionWithNewKeyspace();
        loadFromFile("reflexiveSymmetricRelation.gql", reflexiveSymmetricRelationSession);
        typeDerivationSession = server.sessionWithNewKeyspace();
        loadFromFile("typeDerivation.gql", typeDerivationSession);
        typeDerivationWithDirectSession = server.sessionWithNewKeyspace();
        loadFromFile("typeDerivationWithDirect.gql", typeDerivationWithDirectSession);

        typeDerivationRelationsWithDirectSession = server.sessionWithNewKeyspace();
        loadFromFile("typeDerivationRelationsWithDirect.gql", typeDerivationRelationsWithDirectSession);
        typeDerivationFromAttributeSession = server.sessionWithNewKeyspace();
        loadFromFile("typeDerivationFromAttribute.gql", typeDerivationFromAttributeSession);
        typeDerivationFromRelationsSession = server.sessionWithNewKeyspace();
        loadFromFile("typeDerivationFromRelations.gql", typeDerivationFromRelationsSession);

        //currently disallowed by rule validation
        /*
        freshEntityDerivationSession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit("freshEntityDerivation.gql", freshEntityDerivationSession);
        freshEntityDerivationFromRelationsSession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit("freshEntityDerivationFromRelations.gql", freshEntityDerivationFromRelationsSession);
        freshRelationDerivationSession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit("freshRelationDerivation.gql", freshRelationDerivationSession);
        */

        appendingRPsContextSession = server.sessionWithNewKeyspace();
        loadFromFile("appendingRPs.gql", appendingRPsContextSession);
        resourceAttachmentSession = server.sessionWithNewKeyspace();
        loadFromFile("resourceAttachment.gql", resourceAttachmentSession);
        resourcesAsRolePlayersSession = server.sessionWithNewKeyspace();
        loadFromFile("resourcesAsRolePlayers.gql", resourcesAsRolePlayersSession);

        test7Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet7.gql", test7Session);
        test8Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet8.gql", test8Session);
        test9Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet9.gql", test9Session);
        test10Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet10.gql", test10Session);
        test11Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet11.gql", test11Session);
        test12Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet12.gql", test12Session);
        test13Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet13.gql", test13Session);
        test19Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet19.gql", test19Session);
        test19recursiveSession = server.sessionWithNewKeyspace();
        loadFromFile("testSet19-recursive.gql", test19recursiveSession);
        test20Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet20.gql", test20Session);
        test21Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet21.gql", test21Session);
        test22Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet22.gql", test22Session);

        test23Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet23.gql", test23Session);
        test24Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet24.gql", test24Session);
        test25Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet25.gql", test25Session);
        test26Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet26.gql", test26Session);
        test27Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet27.gql", test27Session);
        test28Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet28.gql", test28Session);
        test28bSession = server.sessionWithNewKeyspace();
        loadFromFile("testSet28b.gql", test28bSession);

        test29Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet29.gql", test29Session);
        test30Session = server.sessionWithNewKeyspace();
        loadFromFile("testSet30.gql", test30Session);
        resourceOwnershipSession = server.sessionWithNewKeyspace();
        loadFromFile("resourceOwnership.gql", resourceOwnershipSession);
        resourceHierarchySession = server.sessionWithNewKeyspace();
        loadFromFile("resourceHierarchy.gql", resourceHierarchySession);
        resourceDirectionalitySession = server.sessionWithNewKeyspace();
        loadFromFile("resourceDirectionality.gql", resourceDirectionalitySession);
    }

    private static void loadFromFile(String fileName, Session session){
        try {
            System.out.println("Loading " + fileName);
            InputStream inputStream = ReasoningIT.class.getClassLoader().getResourceAsStream("test-integration/graql/reasoner/stubs/" + fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            tx.graql().parser().parseList(s).forEach(Query::execute);
            tx.commit();
        } catch (Exception e){
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    //The tests validate the correctness of the rule reasoning implementation w.r.t. the intended semantics of rules.
    //The ignored tests reveal some bugs in the reasoning algorithm, as they don't return the expected results,
    //as specified in the respective comments below.

    @Test
    public void attributeOwnerResultsAreConsistentBetweenDifferentAccessPoints(){
        try(TransactionImpl tx = resourceDirectionalitySession.transaction(Transaction.Type.WRITE)) {
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

    @Test
    public void resourceHierarchiesAreRespected() {
        try(TransactionImpl tx = resourceHierarchySession.transaction(Transaction.Type.WRITE)) {
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

    @Test
    public void resourceOwnershipNotPropagatedWithinRelation() {
        try(TransactionImpl tx = resourceOwnershipSession.transaction(Transaction.Type.WRITE)) {
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

    @Test //Expected result: Both queries should return a non-empty result, with $x/$y mapped to a unique entity.
    public void unificationOfReflexiveRelations() {
        try(TransactionImpl tx = reflexiveRelationSession.transaction(Transaction.Type.WRITE)) {
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

    @Test //Expected result: Both queries should return a non-empty result, with $x/$y mapped to a unique entity.
    public void unificationOfReflexiveSymmetricRelations() {
        try(TransactionImpl tx = reflexiveSymmetricRelationSession.transaction(Transaction.Type.WRITE)) {
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

    @Test //Expected result: The query should return a unique match.
    public void generatingMultipleIsaEdges() {
        try(TransactionImpl tx = typeDerivationSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match $x isa derivedEntity; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(1, answers.size());
        }
    }

    @Test //Expected result: Differentiated behaviour based on directedness of the isa.
    public void generatingIsaEdgesDirectly() {
        try(TransactionImpl tx = typeDerivationWithDirectSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
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
    }

    @Test //Expected result: Differentiated behaviour based on directedness of the isa.
    public void generatingIsaEdgesForRelationsDirectly() {
        try(TransactionImpl tx = typeDerivationRelationsWithDirectSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
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
    }

    @Test //Expected result: The query should return 3 results: one for meta type, one for db, one for inferred type.
    public void queryingForGenericType_ruleDefinesNewType() {
        try(TransactionImpl tx = typeDerivationSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match $x isa $type; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(4, answers.size());
            answers.forEach(ans -> assertEquals(2, ans.size()));
        }
    }

    @Test //Expected result: The queries should return the same two matches.
    public void generatingIsaEdgeFromRelations() {
        try(TransactionImpl tx = typeDerivationFromRelationsSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match $x isa baseEntity; get;";
            String queryString2 = "match $x isa derivedEntity; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
            assertEquals(2, answers.size());
            assertTrue(answers.containsAll(answers2));
            assertTrue(answers2.containsAll(answers));
        }
    }

    @Test //Expected result: The queries should return the same two matches.
    public void generatingIsaEdgeFromAttribute() {
        try(TransactionImpl tx = typeDerivationFromAttributeSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match $x isa baseEntity; get;";
            String queryString2 = "match $x isa derivedEntity; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
            assertEquals(tx.getAttributeType("baseAttribute").instances().count(), answers.size());
            assertTrue(answers.containsAll(answers2));
            assertTrue(answers2.containsAll(answers));
        }
    }

    //TODO: currently disallowed by rule validation
    @Ignore
    @Test //Expected result: The queries should return different matches, unique per query.
    public void generatingFreshEntity() {
        try(TransactionImpl tx = freshEntityDerivationSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match $x isa baseEntity; get;";
            String queryString2 = "match $x isa derivedEntity; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
            assertEquals(answers.size(), answers2.size());
            assertFalse(answers.containsAll(answers2));
            assertFalse(answers2.containsAll(answers));
        }
    }

    //TODO: currently disallowed by rule validation
    @Ignore
    @Test
    //Expected result: The query should return a unique match (or possibly nothing if we enforce range-restriction).
    public void generatingFreshEntity2() {
        try(TransactionImpl tx = freshEntityDerivationFromRelationsSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(false);
            QueryBuilder iqb = tx.graql().infer(true);
            String queryString = "match $x isa derivedEntity; get;";
            String explicitQuery = "match $x isa baseEntity; get;";
            List<ConceptMap> answers = iqb.<GetQuery>parse(queryString).execute();
            List<ConceptMap> answers2 = qb.<GetQuery>parse(explicitQuery).execute();

            assertEquals(3, answers2.size());
            assertTrue(!answers2.containsAll(answers));
        }
    }

    //TODO: currently disallowed by rule validation
    @Ignore
    @Test //Expected result: The query should return three different instances of relation1 with unique ids.
    public void generatingFreshRelation() {
        try(TransactionImpl tx = freshRelationDerivationSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match $x isa baseRelation; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(3, answers.size());
        }
    }

    @Test //Expected result: The query should return 10 unique matches (no duplicates).
    public void distinctLimitedAnswersOfInfinitelyGeneratingRule() {
        try(TransactionImpl tx = test7Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder iqb = tx.graql().infer(true);
            QueryBuilder qb = tx.graql().infer(false);
            String queryString = "match $x isa relation1; limit 10; get;";
            List<ConceptMap> answers = iqb.<GetQuery>parse(queryString).execute();
            assertEquals(10, answers.size());
            assertEquals(qb.<GetQuery>parse(queryString).execute().size(), answers.size());
        }
    }

    @Test //Expected result: The query should not return any matches (or possibly return a single match with $x=$y)
    public void roleUnificationWithRoleHierarchiesInvolved() {
        try(TransactionImpl tx = test8Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match (role2:$x, role3:$y) isa relation2; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertThat(answers, empty());
        }
    }

    @Test //Expected result: The query should not return any matches (or possibly return a single match with $x=$y)
    public void roleUnificationWithRepeatingRoleTypes() {
        try(TransactionImpl tx = test9Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match (role1:$x, role1:$y) isa relation2; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertThat(answers, empty());
        }
    }

    @Test //Expected result: The query should return a single match
    public void roleUnificationWithLessRelationPlayersInQueryThanHead() {
        try(TransactionImpl tx = test9Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match (role1:$x) isa relation2; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(1, answers.size());
        }
    }

    /**
     * recursive relation having same type for different role players
     * tests for handling recursivity and equivalence of queries and relations
     */
    @Test //Expected result: The query should return a unique match
    public void transRelationWithEntityGuardsAtBothEnds() {
        try(TransactionImpl tx = test10Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match (role1: $x, role2: $y) isa relation2; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(1, answers.size());
        }
    }

    @Test //Expected result: The query should return a unique match
    public void transRelationWithRelationGuardsAtBothEnds() {
        try(TransactionImpl tx = test11Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match (role1:$x, role2:$y) isa relation3; get;";
            assertEquals(1, qb.<GetQuery>parse(queryString).execute().size());
        }
    }

    @Test //Expected result: The query should return two unique matches
    public void circularRuleDependencies() {
        try(TransactionImpl tx = test12Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match (role1:$x, role2:$y) isa relation3; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(2, answers.size());
        }
    }

    @Test //Expected result: The query should return a unique match
    public void rulesInteractingWithTypeHierarchy() {
        try(TransactionImpl tx = test13Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match (role1:$x, role2:$y) isa relation2; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(1, answers.size());
        }
    }

    @Test
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_reattachingResourceToEntity() {
        try(TransactionImpl tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);

            String queryString = "match $x isa genericEntity, has reattachable-resource-string $y; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            String queryString2 = "match $x isa reattachable-resource-string; get;";
            List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();

            assertEquals(tx.getEntityType("genericEntity").instances().count(), answers.size());
            assertEquals(1, answers2.size());
        }
    }

    @Test
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_queryingForGenericRelation() {
        try(TransactionImpl tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);

            String queryString = "match $x isa genericEntity;($x, $y); get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();

            assertEquals(3, answers.size());
            assertEquals(2, answers.stream().filter(answer -> answer.get("y").isAttribute()).count());
        }
    }

    @Test
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_usingExistingResourceToDefineSubResource() {
        try(TransactionImpl tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match $x isa genericEntity, has subResource $y; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(tx.getEntityType("genericEntity").instances().count(), answers.size());

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
    }

    @Test
    public void whenReasoningWithResourcesInRelationForm_ResultsAreComplete() {
        try(TransactionImpl tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);

            List<ConceptMap> concepts = qb.<GetQuery>parse("match $x isa genericEntity;get;").execute();
            List<ConceptMap> subResources = qb.<GetQuery>parse(
                    "match $x isa genericEntity has subResource $res; get;").execute();

            String queryString = "match " +
                    "$rel($role:$x) isa @has-reattachable-resource-string;" +
                    "$x isa genericEntity;" +
                    "get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            //base resource yield 3 roles: metarole, base attribute rule, specific role
            //subresources yield 4 roles: all the above + specialised role
            assertEquals(concepts.size() * 3 + subResources.size() * 4, answers.size());
            answers.forEach(ans -> assertEquals(3, ans.size()));
        }
    }

    //TODO leads to cache inconsistency
    @Ignore
    @Test
    public void whenReasoningWithResourcesWithRelationVar_ResultsAreComplete() {
        try(TransactionImpl tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);

            VarPattern has = var("x").has(Label.of("reattachable-resource-string"), var("y"), var("r"));
            List<ConceptMap> answers = qb.match(has).get().execute();
            assertEquals(3, answers.size());
            answers.forEach(a -> assertTrue(a.vars().contains(var("r"))));
        }
    }

    @Test
    public void whenExecutingAQueryWithImplicitTypes_InferenceHasAtLeastAsManyResults() {
        try(TransactionImpl tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder withInference = tx.graql().infer(true);
            QueryBuilder withoutInference = tx.graql().infer(false);

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
    }

    @Test
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_attachingExistingResourceToARelation() {
        try(TransactionImpl tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);

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
    }

    @Test
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_derivingResourceFromOtherResourceWithConditionalValue() {
        try(TransactionImpl tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match $x has derived-resource-boolean $r; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(1, answers.size());
        }
    }

    @Test
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void derivingResourceWithSpecificValue() {
        try(TransactionImpl tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
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
    }

    @Test
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_attachingStrayResourceToEntityDoesntThrowErrors() {
        try(TransactionImpl tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match $x isa yetAnotherEntity, has derived-resource-string 'unattached'; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(2, answers.size());
        }
    }

    @Test
    public void resourcesAsRolePlayers() {
        try(TransactionImpl tx = resourcesAsRolePlayersSession.transaction(Transaction.Type.WRITE)) {
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

    @Test
    public void resourcesAsRolePlayers_vpPropagationTest() {
        try(TransactionImpl tx = resourcesAsRolePlayersSession.transaction(Transaction.Type.WRITE)) {
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

            assertEquals(answers.size(), 3);
            assertEquals(answers2.size(), 3);
            assertEquals(answers3.size(), 3);
            assertEquals(answers4.size(), 3);
            assertEquals(answers5.size(), answers.size() + answers2.size() + answers3.size() + answers4.size());
            assertEquals(answers6.size(), answers5.size() - answers4.size());
        }
    }

    @Test //Expected result: Two answers obtained only if the rule query containing sub type is correctly executed.
    public void instanceTypeHierarchyRespected_queryHasSuperTypes(){
        try(TransactionImpl tx = test19Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match " +
                    "$x isa entity1;" +
                    "$y isa entity1;" +
                    "(role1: $x, role2: $y) isa relation1;";
            String queryString2 = queryString + "$y has name 'a';";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString + " get;").execute();
            assertEquals(2, answers.size());
            List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2 + " get;").execute();
            assertEquals(2, answers2.size());
        }
    }

    @Test //Expected result: Single answer obtained only if the rule query containing super type is correctly executed.
    public void instanceTypeHierarchyRespected_querySpecialisesType(){
        try(TransactionImpl tx = test19Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match " +
                    "$x isa entity1;" +
                    "$y isa subEntity1;" +
                    "(role1: $x, role2: $y) isa relation1;";
            String queryString2 = queryString + "$y has name 'a';";

            List<ConceptMap> answers = qb.<GetQuery>parse(queryString + " get;").execute();
            assertEquals(1, answers.size());
            List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2 + " get;").execute();
            assertEquals(1, answers2.size());
        }
    }

    @Test //Expected result: Single answer obtained only if the rule query containing super type is correctly executed.
    public void instanceTypeHierarchyRespected_queryOverwritesTypes(){
        try(TransactionImpl tx = test19Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match " +
                    "$x isa subEntity1;" +
                    "$y isa entity1;" +
                    "(role1: $x, role2: $y) isa relation1;";
            String queryString2 = queryString + "$y has name 'a';";

            List<ConceptMap> answers = qb.<GetQuery>parse(queryString + "get;").execute();
            assertEquals(2, answers.size());
            List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2 + "get;").execute();
            assertEquals(2, answers2.size());
        }
    }

    @Test //Expected result: Two answers obtained only if the rule query containing sub type is correctly executed.
    public void instanceTypeHierarchyRespected_queryHasSuperTypes_recursiveRule(){
        try(TransactionImpl tx = test19recursiveSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match " +
                    "$x isa entity1;" +
                    "$y isa entity1;" +
                    "(role1: $x, role2: $y) isa relation1;";
            String queryString2 = queryString + "$y has name 'a';";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString + " get;").execute();
            assertEquals(2, answers.size());
            List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2 + " get;").execute();
            assertEquals(2, answers2.size());
        }
    }

    @Test //Expected result: Single answer obtained only if the rule query containing super type is correctly executed.
    public void instanceTypeHierarchyRespected_querySpecialisesType_recursiveRule(){
        try(TransactionImpl tx = test19recursiveSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match " +
                    "$x isa entity1;" +
                    "$y isa subEntity1;" +
                    "(role1: $x, role2: $y) isa relation1;";
            String queryString2 = queryString + "$y has name 'a';";

            List<ConceptMap> answers = qb.<GetQuery>parse(queryString + " get;").execute();
            assertEquals(1, answers.size());
            List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2 + " get;").execute();
            assertEquals(1, answers2.size());
        }
    }

    @Test //Expected result: Single answer obtained only if the rule query containing super type is correctly executed.
    public void instanceTypeHierarchyRespected_queryOverwritesTypes_recursiveRule(){
        try(TransactionImpl tx = test19recursiveSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match " +
                    "$x isa subEntity1;" +
                    "$y isa entity1;" +
                    "(role1: $x, role2: $y) isa relation1;";
            String queryString2 = queryString + "$y has name 'a';";

            List<ConceptMap> answers = qb.<GetQuery>parse(queryString + " get;").execute();
            assertEquals(2, answers.size());
            List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2 + " get;").execute();
            assertEquals(2, answers2.size());
        }
    }

    @Test //Expected result: Both queries should return a single equal match as they trigger the same rule.
    public void reasoningOverRelationHierarchy(){
        try(TransactionImpl tx = test20Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match (role1: $x, role2: $y) isa relation1; get;";
            String queryString2 = "match (role1: $x, role2: $y) isa sub-relation1; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
            assertEquals(1, answers.size());
            assertTrue(answers.containsAll(answers2));
            assertTrue(answers2.containsAll(answers));
        }
    }

    @Test //Expected result: Both queries should return a single equal match as they trigger the same rule.
    public void reasoningOverEntityHierarchy(){
        try(TransactionImpl tx = test21Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match $x isa baseEntity; get;";
            String queryString2 = "match $x isa subEntity; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            List<ConceptMap> answers2 = qb.<GetQuery>parse(queryString2).execute();
            assertEquals(1, answers.size());
            assertTrue(answers.containsAll(answers2));
            assertTrue(answers2.containsAll(answers));
        }
    }

    @Test //Expected result: Returns db and inferred relations + their inverses and relations with self for all entities
    public void reasoningWithRepeatingRoles(){
        try(TransactionImpl tx = test22Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match (friend:$x1, friend:$x2) isa knows-trans; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(16, answers.size());
        }
    }

    @Test //Expected result: The same set of results is always returned
    public void reasoningWithLimitHigherThanNumberOfResults_ReturnsConsistentResults(){
        try(TransactionImpl tx = test23Session.transaction(Transaction.Type.WRITE)) {
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

    @Test //Expected result: Relations between all entity instances including relation between each instance and itself
    public void reasoningWithEntityTypes() {
        try(TransactionImpl tx = test24Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            QueryBuilder qbm = tx.graql().infer(true);
            String queryString = "match (role1:$x1, role2:$x2) isa relation1; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            List<ConceptMap> answers2 = qbm.<GetQuery>parse(queryString).execute();
            assertEquals(9, answers.size());
            assertEquals(9, answers2.size());
            assertCollectionsEqual(answers, answers2);
        }
    }

    @Test //Expected result: Relations between all entity instances including relation between each instance and itself
    public void reasoningWithEntityTypes_WithNeqProperty() {
        try(TransactionImpl tx = test24Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match (role1:$x1, role2:$x2) isa relation2; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(6, answers.size());
        }
    }

    @Test //Expected result: Timeline is correctly recognised via applying resource comparisons in the rule body
    public void reasoningWithResourceValueComparison() {
        try(TransactionImpl tx = test25Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match (predecessor:$x1, successor:$x2) isa message-succession; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(10, answers.size());
        }
    }

    //tests if partial substitutions are propagated correctly - atom disjointness may lead to variable loss (bug #15476)
    @Test //Expected result: 2 relations obtained by correctly finding reified relations
    public void reasoningWithReifiedRelations() {
        try(TransactionImpl tx = test26Session.transaction(Transaction.Type.WRITE)) {
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

    @Test //Expected result: 2 relations obtained by correctly finding reified relations
    public void reasoningWithNeqProperty() {
        try(TransactionImpl tx = test27Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match (related-state: $s) isa holds; get;";

            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            List<ConceptMap> exact = qb.<GetQuery>parse("match $s isa state, has name 's2'; get;").execute();
            assertCollectionsEqual(exact, answers);
        }
    }

    @Test //Expected result: number of answers equal to specified limit (no duplicates produced)
    public void duplicatesNotProducedWhenResolvingNonResolvableConjunctionsWithoutType(){
        try(TransactionImpl tx = test28Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match " +
                    "(role1: $x, role2: $y);" +
                    "(role1: $y, role2: $z);" +
                    "(role3: $z, role4: $w) isa relation3;" +
                    "limit 3; get;";

            assertEquals(3, qb.<GetQuery>parse(queryString).execute().size());
        }
    }

    @Test //Expected result: no answers (if types were incorrectly inferred the query would yield answers)
    public void relationTypesAreCorrectlyInferredInConjunction_TypeArePresent(){
        try(TransactionImpl tx = test28Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match " +
                    "(role1: $x, role2: $y) isa relation1;" +
                    "(role1: $y, role2: $z) isa relation1;" +
                    "(role3: $z, role4: $w) isa relation3; get;";

            assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
        }
    }

    @Test
    public void relationTypesAreCorrectlyInferredInConjunction_TypesAreAbsent(){
        try(TransactionImpl tx = test28bSession.transaction(Transaction.Type.WRITE)) {
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

    @Test
    public void relationTypesAreCorrectlyInferredInConjunction_TypesAreAbsent_DisconnectedQuery(){
        try(TransactionImpl tx = test28bSession.transaction(Transaction.Type.WRITE)) {
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

    /**
       Should find the possible relation configurations:
         (x, z) - (z, z1) - (z1, z)
                - (z, z2) - (z2, z)
                - (z, y)  - { (y,z) (y, x) }
                - (z, x)  - { res, (x, y), (x, z) }
         */
    @Test
    public void relationTypesAreCorrectlyInferredInConjunction_TypesAreAbsent_WithRelationWithoutAnyBounds(){
        try(TransactionImpl tx = test28bSession.transaction(Transaction.Type.WRITE)) {
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

    @Test //when rule are defined to append new RPs no new relation instances should be created
    public void whenAppendingRolePlayers_noNewRelationsAreCreated(){
        try(TransactionImpl tx = appendingRPsContextSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql();

            List<ConceptMap> answers = qb.infer(false).<GetQuery>parse("match $r isa relation; get;").execute();
            List<ConceptMap> inferredAnswers = qb.infer(true).<GetQuery>parse("match $r isa relation; get;").execute();
            assertEquals(answers, inferredAnswers);
        }
    }

    @Test
    public void whenQueryingAppendedRelations_rulesAreMatchedCorrectly(){
        try(TransactionImpl tx = appendingRPsContextSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            Set<ConceptMap> variants = Stream.of(
                    Iterables.getOnlyElement(qb.<GetQuery>parse("match $r (someRole: $x, anotherRole: $y, anotherRole: $z, inferredRole: $z); $y != $z;get;").execute()),
                    Iterables.getOnlyElement(qb.<GetQuery>parse("match $r (someRole: $x, inferredRole: $z ); $x has resource 'value'; get;").execute()),
                    Iterables.getOnlyElement(qb.<GetQuery>parse("match $r (someRole: $x, yetAnotherRole: $y, andYetAnotherRole: $y, inferredRole: $z); get;").execute()),
                    Iterables.getOnlyElement(qb.<GetQuery>parse("match $r (anotherRole: $x, andYetAnotherRole: $y); get;").execute())
            )
                    .map(ans -> ans.project(Sets.newHashSet(var("r"))))
                    .collect(Collectors.toSet());

            List<ConceptMap> answers = qb.<GetQuery>parse("match $r isa relation; get;").execute();
            assertCollectionsEqual(variants, answers);
        }
    }

    @Test
    public void whenRuleContainsRelationRequiringAppend_bodyIsRewrittenCorrectly(){
        try(TransactionImpl tx = appendingRPsContextSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            List<ConceptMap> answers = qb.<GetQuery>parse("match (inferredRole: $x, inferredRole: $y, inferredRole: $z) isa derivedRelation; get;").execute();
            assertEquals(2, answers.size());
        }
    }

    @Test //tests a query containing a neq predicate bound to a recursive relation
    public void recursiveRelationWithNeqPredicate(){
        try(TransactionImpl tx = test29Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String baseQueryString = "match " +
                    "(role1: $x, role2: $y) isa binary-base;" +
                    "$x != $y;";
            String queryString = baseQueryString + "$y has name 'c'; get;";

            List<ConceptMap> baseAnswers = qb.<GetQuery>parse(baseQueryString + "get;").execute();
            assertEquals(6, baseAnswers.size());
            baseAnswers.forEach(ans -> {
                assertEquals(2, ans.size());
                assertNotEquals(ans.get("x"), ans.get("y"));
            });

            String explicitString = "match " +
                    "(role1: $x, role2: $y) isa binary-base;" +
                    "$y has name 'c';" +
                    "{$x has name 'a';} or {$x has name 'b';}; get;";

            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            List<ConceptMap> answers2 = qb.<GetQuery>parse(explicitString).execute();
            assertCollectionsEqual(answers, answers2);
        }
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
        try(TransactionImpl tx = test29Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String baseQueryString = "match " +
                    "(role1: $x, role2: $y) isa binary-base;" +
                    "(role1: $x, role2: $z) isa binary-base;" +
                    "$y != $z;";

            List<ConceptMap> baseAnswers = qb.<GetQuery>parse(baseQueryString + "get;").execute();
            assertEquals(18, baseAnswers.size());
            baseAnswers.forEach(ans -> {
                assertEquals(3, ans.size());
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
            assertCollectionsEqual(answers, answers2);
        }
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
        try(TransactionImpl tx = test29Session.transaction(Transaction.Type.WRITE)){
            QueryBuilder qb = tx.graql().infer(true);
            String baseQueryString = "match " +
                    "(role1: $x, role2: $y) isa binary-base;" +
                    "(role1: $y, role2: $z) isa binary-base;" +
                    "$x != $z;";

            List<ConceptMap> baseAnswers = qb.<GetQuery>parse(baseQueryString + "get;").execute();
            assertEquals(18, baseAnswers.size());
            baseAnswers.forEach(ans -> {
                assertEquals(3, ans.size());
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
            assertCollectionsEqual(answers, answers2);
        }
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
        try(TransactionImpl tx = test29Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String baseQueryString = "match " +
                    "(role1: $x, role2: $y1) isa binary-base;" +
                    "(role1: $x, role2: $z1) isa binary-base;" +
                    "(role1: $x, role2: $y2) isa binary-base;" +
                    "(role1: $x, role2: $z2) isa binary-base;" +

                    "$y1 != $z1;" +
                    "$y2 != $z2;";

            List<ConceptMap> baseAnswers = qb.<GetQuery>parse(baseQueryString + "get;").execute();
            assertEquals(108, baseAnswers.size());
            baseAnswers.forEach(ans -> {
                assertEquals(5, ans.size());
                assertNotEquals(ans.get("y1"), ans.get("z1"));
                assertNotEquals(ans.get("y2"), ans.get("z2"));
            });

            String queryString = baseQueryString + "$x has name 'a';";

            List<ConceptMap> answers = qb.<GetQuery>parse(queryString + "get;").execute();
            assertEquals(36, answers.size());
            answers.forEach(ans -> {
                assertEquals(5, ans.size());
                assertNotEquals(ans.get("y1"), ans.get("z1"));
                assertNotEquals(ans.get("y2"), ans.get("z2"));
            });
        }
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
        try(TransactionImpl tx = test29Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String baseQueryString = "match " +
                    "(role1: $x, role2: $y) isa binary-base;" +
                    "$x != $z1;" +
                    "$y != $z2;" +
                    "(role1: $x, role2: $z1) isa binary-base;" +
                    "(role1: $y, role2: $z2) isa binary-base;";

            List<ConceptMap> baseAnswers = qb.<GetQuery>parse(baseQueryString + "get;").execute();
            assertEquals(36, baseAnswers.size());
            baseAnswers.forEach(ans -> {
                assertEquals(4, ans.size());
                assertNotEquals(ans.get("x"), ans.get("z1"));
                assertNotEquals(ans.get("y"), ans.get("z2"));
            });

            String queryString = baseQueryString + "$x has name 'a';";

            List<ConceptMap> answers = qb.<GetQuery>parse(queryString + "get;").execute();
            assertEquals(12, answers.size());
            answers.forEach(ans -> {
                assertEquals(4, ans.size());
                assertNotEquals(ans.get("x"), ans.get("z1"));
                assertNotEquals(ans.get("y"), ans.get("z2"));
            });
        }
    }

    @Test //tests whether shared resources are recognised correctly
    public void inferrableRelationWithRolePlayersSharingResource(){
        try(TransactionImpl tx = test29Session.transaction(Transaction.Type.WRITE)) {
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

    @Test
    public void ternaryRelationsRequiryingDifferentMultiunifiers(){
        try(TransactionImpl tx = test29Session.transaction(Transaction.Type.WRITE)) {
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

    @Test
    public void binaryRelationWithDifferentVariantsOfVariableRoles(){
        try(TransactionImpl tx = test29Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);

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
            assertEquals(18, answers.size());
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
            assertEquals(27, answers2.size());
            assertCollectionsEqual(answers2, equivalentAnswers2);

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
            assertEquals(9, answers3.size());
            assertCollectionsEqual(answers3, equivalentAnswers3);

            //9 relation instances with 7 possible permutations for each - 63 answers
            String queryString4 = "match " +
                    "($r1: $a, $r2: $b) isa binary-base;" +
                    "get;";

            List<ConceptMap> answers4 = qb.<GetQuery>parse(queryString4).execute();
            assertEquals(63, answers4.size());
        }
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
        try(TransactionImpl tx = test29Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            final int arity = (int) tx.getRelationshipType(label).roles().count();

            VarPattern resourcePattern = var("a1").has("name", "a");

            //This query generalises all roles but the first one.
            VarPattern pattern = var().rel("role1", "a1");
            for (int i = 2; i <= arity; i++) pattern = pattern.rel(var("r" + i), "a" + i);
            pattern = pattern.isa(label);

            List<ConceptMap> answers = qb.match(pattern.and(resourcePattern)).get().execute();
            assertEquals(answerCombinations(arity - 1, conceptDOF), answers.size());

            //We get extra conceptDOF degrees of freedom by removing the resource constraint on $a1 and the set is symmetric.
            List<ConceptMap> answers2 = qb.match(pattern).get().execute();
            assertEquals(answerCombinations(arity - 1, conceptDOF) * conceptDOF, answers2.size());


            //The general case of mapping all available Rps
            VarPattern generalPattern = var();
            for (int i = 1; i <= arity; i++) generalPattern = generalPattern.rel(var("r" + i), "a" + i);
            generalPattern = generalPattern.isa(label);

            List<ConceptMap> answers3 = qb.match(generalPattern).get().execute();
            assertEquals(answerCombinations(arity, conceptDOF), answers3.size());
        }
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

    @Test
    //tests scenario where rules define mutually recursive relation and resource and we query for an attributed type corresponding to the relation
    public void mutuallyRecursiveRelationAndResource_queryForAttributedType(){
        try(TransactionImpl tx = test30Session.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);

            String queryString = "match $p isa pair, has name 'ff'; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(16, answers.size());
        }
    }

    private static <T> void assertCollectionsEqual(Collection<T> c1, Collection<T> c2) {
        assertTrue(isEqualCollection(c1, c2));
    }
}
