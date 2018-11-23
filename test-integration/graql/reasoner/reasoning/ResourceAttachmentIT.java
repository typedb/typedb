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

package grakn.core.graql.reasoner.reasoning;

import com.google.common.collect.Sets;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.QueryBuilder;
import grakn.core.graql.query.VarPattern;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Label;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import static grakn.core.graql.query.Graql.label;
import static grakn.core.graql.query.Graql.var;
import static grakn.core.graql.internal.Schema.ImplicitType.HAS;
import static grakn.core.graql.internal.Schema.ImplicitType.HAS_OWNER;
import static grakn.core.graql.internal.Schema.ImplicitType.HAS_VALUE;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Suite of tests checking different meanders and aspects of reasoning - full reasoning cycle is being tested.
 */
@SuppressWarnings("CheckReturnValue")
public class ResourceAttachmentIT {

    private static String resourcePath = "test-integration/graql/reasoner/stubs/";

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl resourceAttachmentSession;
    @BeforeClass
    public static void loadContext(){
        resourceAttachmentSession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit(resourcePath, "resourceAttachment.gql", resourceAttachmentSession);
    }

    @AfterClass
    public static void closeSession(){
        resourceAttachmentSession.close();

    }

    @Test
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingResources_reattachingResourceToEntity() {
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
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
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
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
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
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
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
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
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);

            VarPattern has = var("x").has(Label.of("reattachable-resource-string"), var("y"), var("r"));
            List<ConceptMap> answers = qb.match(has).get().execute();
            assertEquals(3, answers.size());
            answers.forEach(a -> assertTrue(a.vars().contains(var("r"))));
        }
    }

    @Test
    public void whenExecutingAQueryWithImplicitTypes_InferenceHasAtLeastAsManyResults() {
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
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
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
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
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match $x has derived-resource-boolean $r; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(1, answers.size());
        }
    }

    @Test
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void derivingResourceWithSpecificValue() {
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
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
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql().infer(true);
            String queryString = "match $x isa yetAnotherEntity, has derived-resource-string 'unattached'; get;";
            List<ConceptMap> answers = qb.<GetQuery>parse(queryString).execute();
            assertEquals(2, answers.size());
        }
    }

}
