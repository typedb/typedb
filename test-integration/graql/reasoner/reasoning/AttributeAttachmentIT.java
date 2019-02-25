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
import grakn.core.concept.answer.ConceptMap;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static grakn.core.server.kb.Schema.ImplicitType.HAS;
import static grakn.core.server.kb.Schema.ImplicitType.HAS_OWNER;
import static grakn.core.server.kb.Schema.ImplicitType.HAS_VALUE;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
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
public class AttributeAttachmentIT {

    private static String resourcePath = "test-integration/graql/reasoner/stubs/";

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl attributeAttachmentSession;
    @BeforeClass
    public static void loadContext(){
        attributeAttachmentSession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit(resourcePath, "resourceAttachment.gql", attributeAttachmentSession);
    }

    @AfterClass
    public static void closeSession(){
        attributeAttachmentSession.close();

    }

    @Test
    //Expected result: When the head of a rule contains attribute assertions, the respective unique attributes should be generated or reused.
    public void reusingAttribute_reattachingAttributeToEntity() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {

            String queryString = "match $x isa genericEntity, has reattachable-resource-string $y; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            String queryString2 = "match $x isa reattachable-resource-string; get;";
            List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());

            assertEquals(tx.getEntityType("genericEntity").instances().count(), answers.size());
            assertEquals(1, answers2.size());
        }
    }

    @Test
    //Expected result: When the head of a rule contains attribute assertions, the respective unique attributes should be generated or reused.
    public void reusingAttributes_queryingForGenericRelation() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {

            String queryString = "match $x isa genericEntity;($x, $y); get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());

            assertEquals(3, answers.size());
            assertEquals(2, answers.stream().filter(answer -> answer.get("y").isAttribute()).count());
        }
    }

    @Test
    //Expected result: When the head of a rule contains attribute assertions, the respective unique attributes should be generated or reused.
    public void reusingAttributes_usingExistingAttributeToDefineSubAttribute() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
                        String queryString = "match $x isa genericEntity, has subResource $y; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(tx.getEntityType("genericEntity").instances().count(), answers.size());

            String queryString2 = "match $x isa subResource; get;";
            List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());
            assertEquals(1, answers2.size());
            assertTrue(answers2.iterator().next().get("x").isAttribute());

            String queryString3 = "match $x isa reattachable-resource-string; $y isa subResource;get;";
            List<ConceptMap> answers3 = tx.execute(Graql.parse(queryString3).asGet());
            assertEquals(1, answers3.size());

            assertTrue(answers3.iterator().next().get("x").isAttribute());
            assertTrue(answers3.iterator().next().get("y").isAttribute());
        }
    }

    @Test
    public void whenReasoningWithAttributesInRelationForm_ResultsAreComplete() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {

            List<ConceptMap> concepts = tx.execute(Graql.parse("match $x isa genericEntity; get;").asGet());
            List<ConceptMap> subResources = tx.execute(Graql.parse(
                    "match $x isa genericEntity, has subResource $res; get;").asGet());
            List<ConceptMap> derivedResources = tx.execute(Graql.parse(
                    "match $x isa genericEntity, has derived-resource-string $res; get;").asGet());

            String queryString = "match " +
                    "$rel($role:$x) isa @has-reattachable-resource-string; " +
                    "$x isa genericEntity; " +
                    "get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            //base resources yield 4 roles: metarole, base attribute role, super role, specific role
            //subresources yield 5 roles: all the above + specialised role
            assertEquals(concepts.size() * 4 + subResources.size() * 5, answers.size());
            answers.forEach(ans -> assertEquals(3, ans.size()));
        }
    }

    //TODO leads to cache inconsistency
    @Ignore
    @Test
    public void whenReasoningWithAttributesWithRelationVar_ResultsAreComplete() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {

            Statement has = var("x").has("reattachable-resource-string", var("y"), var("r"));
            List<ConceptMap> answers = tx.execute(Graql.match(has).get());
            assertEquals(3, answers.size());
            answers.forEach(a -> assertTrue(a.vars().contains(new Variable("r"))));
        }
    }

    @Test
    public void whenExecutingAQueryWithImplicitTypes_InferenceHasAtLeastAsManyResults() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {

            Statement owner = type(HAS_OWNER.getLabel("reattachable-resource-string").getValue());
            Statement value = type(HAS_VALUE.getLabel("reattachable-resource-string").getValue());
            Statement hasRes = type(HAS.getLabel("reattachable-resource-string").getValue());

            GraqlGet query = Graql.match(
                    var().rel(owner, "x").rel(value, "y").isa(hasRes),
                    var("a").has("reattachable-resource-string", var("b"))  // This pattern is added only to encourage reasoning to activate
            ).get();


            Set<ConceptMap> resultsWithoutInference = tx.stream(query,false).collect(toSet());
            Set<ConceptMap> resultsWithInference = tx.stream(query).collect(toSet());

            assertThat(resultsWithoutInference, not(empty()));
            assertThat(Sets.difference(resultsWithoutInference, resultsWithInference), empty());
        }
    }

    @Test
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void reusingAttributes_attachingExistingAttributeToARelation() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {

            String queryString = "match $x isa genericEntity, has reattachable-resource-string $y; $z isa relation0; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(2, answers.size());
            answers.forEach(ans ->
                    {
                        assertTrue(ans.get("x").isEntity());
                        assertTrue(ans.get("y").isAttribute());
                        assertTrue(ans.get("z").isRelation());
                    }
            );

            String queryString2 = "match $x isa relation, has reattachable-resource-string $y; get;";
            List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());
            assertEquals(1, answers2.size());
            answers2.forEach(ans ->
                    {
                        assertTrue(ans.get("x").isRelation());
                        assertTrue(ans.get("y").isAttribute());
                    }
            );
        }
    }

    @Test
    //Expected result: When the head of a rule contains attribute assertions, the respective unique attributes should be generated or reused.
    public void reusingAttributes_derivingAttributeFromOtherAttributeWithConditionalValue() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
                        String queryString = "match $x has derived-resource-boolean $r; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(1, answers.size());
        }
    }

    @Test
    //Expected result: When the head of a rule contains attribute assertions, the respective unique attributes should be generated or reused.
    public void derivingAttributeWithSpecificValue() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
                        String queryString = "match $x has derived-resource-string 'value'; get;";
            String queryString2 = "match $x has derived-resource-string $r; get;";
            GraqlGet query = Graql.parse(queryString).asGet();
            GraqlGet query2 = Graql.parse(queryString2).asGet();
            List<ConceptMap> answers = tx.execute(query);
            List<ConceptMap> answers2 = tx.execute(query2);
            List<ConceptMap> requeriedAnswers = tx.execute(query);
            assertEquals(2, answers.size());
            assertEquals(4, answers2.size());
            assertEquals(answers.size(), requeriedAnswers.size());
            assertTrue(answers.containsAll(requeriedAnswers));
        }
    }

    @Test
    //Expected result: When the head of a rule contains attribute assertions, the respective unique attributes should be generated or reused.
    public void reusingAttributes_attachingStrayAttributeToEntityDoesntThrowErrors() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
                        String queryString = "match $x isa yetAnotherEntity, has derived-resource-string 'unattached'; get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
            assertEquals(2, answers.size());
        }
    }

}
