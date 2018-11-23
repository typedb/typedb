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

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.Pattern;
import grakn.core.graql.query.QueryBuilder;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import static grakn.core.util.GraqlTestUtil.assertCollectionsEqual;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;

public class NegationIT {

    private static String resourcePath = "test-integration/graql/reasoner/resources/";

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl resourceAttachmentSession;
    private static SessionImpl resourceHierarchySession;

    @BeforeClass
    public static void loadContext(){
        resourceAttachmentSession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit(resourcePath,"resourceAttachment.gql", resourceAttachmentSession);
        resourceHierarchySession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit(resourcePath,"resourceHierarchy.gql", resourceHierarchySession);
    }

    @AfterClass
    public static void closeSession(){
        resourceAttachmentSession.close();
        resourceHierarchySession.close();
    }

    @Test
    public void testRemovingNesting(){
        try(Transaction tx = resourceHierarchySession.transaction(Transaction.Type.READ)) {
            QueryBuilder qb = tx.graql().infer(true);

            Pattern pattern = qb.parser().parsePattern(
                    "{" +
                            "$x isa genericEntity;" +
                            "NOT {" +
                            "$x has simpleResource 'value';" +
                            "$x has baseResource 'anotherValue';" +
                            "};" +
                            "}"
            );
            Pattern equivalentPattern = qb.parser().parsePattern("{" +
                    "{$x isa genericEntity;" +
                    "NOT $x has simpleResource 'value';} or " +
                    "{$x isa genericEntity;" +
                    "NOT $x has baseResource 'anotherValue';};" +
                    "}"
            );
            assertEquals(pattern.admin().getDisjunctiveNormalForm(), equivalentPattern.admin().getDisjunctiveNormalForm());
        }
    }

    @Test
    public void negatePredicate(){
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.READ)) {
            QueryBuilder qb = tx.graql().infer(true);

            String specificValue = "unattached";
            List<ConceptMap> fullAnswers = qb.<GetQuery>parse("match $x has attribute $r;get;").execute();
            List<ConceptMap> answersWithoutSpecificValue = qb.<GetQuery>parse("match $x has attribute $r; NOT $r == '" + specificValue + "';get;").execute();

            assertCollectionsEqual(
                    fullAnswers.stream().filter(ans -> !ans.get("r").asAttribute().value().equals(specificValue)).collect(toSet()),
                    answersWithoutSpecificValue
            );
        }
    }

    @Test
    public void negateType(){
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.READ)) {
            QueryBuilder qb = tx.graql().infer(true);

            String specificTypeLabel = "yetAnotherEntity";
            EntityType specificType = tx.getEntityType(specificTypeLabel);

            List<ConceptMap> fullAnswers = qb.<GetQuery>parse("match $x has attribute $r;get;").execute();
            List<ConceptMap> answersWithoutSpecificType = qb.<GetQuery>parse("match $x has attribute $r; NOT $x isa " + specificTypeLabel + ";get;").execute();

            assertCollectionsEqual(
                    fullAnswers.stream()
                            .filter(ans -> !ans.get("x").asThing().type().equals(specificType)).collect(toSet()),
                    answersWithoutSpecificType
            );
        }
    }

    @Test
    public void negateRelation(){
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.READ)) {
            QueryBuilder qb = tx.graql().infer(true);

            List<ConceptMap> fullAnswers = qb.<GetQuery>parse("match $x has attribute $r;get;").execute();
            List<ConceptMap> answersNotPlayingInRelation = qb.<GetQuery>parse("match $x has attribute $r; NOT ($x) isa relation;get;").execute();

            assertCollectionsEqual(
                    fullAnswers.stream()
                            .filter(ans -> tx.getRelationshipType("relation").instances().flatMap(i -> i.rolePlayers()).noneMatch(rp -> rp.equals(ans.get("x"))))
                            .collect(toSet()),
                    answersNotPlayingInRelation
            );
        }
    }

    @Test
    public void negateMultipleProperties(){
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.READ)) {
            QueryBuilder qb = tx.graql().infer(true);

            String specificValue = "value";
            String anotherSpecificValue = "unattached";
            String specificTypeLabel = "yetAnotherEntity";
            EntityType specificType = tx.getEntityType(specificTypeLabel);

            List<ConceptMap> fullAnswers = qb.<GetQuery>parse("match $x has attribute $r;get;").execute();

            List<ConceptMap> answersWithoutSpecifcTypeAndValue = qb.<GetQuery>parse(
                    "match " +
                            "$x has attribute $r;" +
                            "NOT $x isa " + specificTypeLabel +
                            " has reattachable-resource-string " + "'" + specificValue + "'" +
                            " has derived-resource-string " + "'" + anotherSpecificValue + "';" +
                            "get;"
            ).execute();

            assertCollectionsEqual(
                    fullAnswers.stream()
                            .filter(ans -> !ans.get("r").asAttribute().value().equals(specificValue))
                            .filter(ans -> !ans.get("r").asAttribute().value().equals(anotherSpecificValue))
                            .filter(ans -> !ans.get("x").asThing().type().equals(specificType))
                            .collect(toSet()),
                    answersWithoutSpecifcTypeAndValue
            );
        }
    }

    @Test
    public void negateMultiplePatterns(){
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.READ)) {
            QueryBuilder qb = tx.graql().infer(true);

            String anotherSpecificValue = "value";
            String specificTypeLabel = "yetAnotherEntity";
            EntityType specificType = tx.getEntityType(specificTypeLabel);

            List<ConceptMap> fullAnswers = qb.<GetQuery>parse("match $x has attribute $r;get;").execute();

            List<ConceptMap> answersWithoutSpecifcTypeAndValue = qb.<GetQuery>parse(
                    "match " +
                            "$x has attribute $r;" +
                            "NOT $x isa " + specificTypeLabel + ";" +
                            "NOT $r == '" + anotherSpecificValue + "';" +
                            "get;"
            ).execute();

            assertCollectionsEqual(
                    fullAnswers.stream()
                            .filter(ans -> !ans.get("r").asAttribute().value().equals(anotherSpecificValue))
                            .filter(ans -> !ans.get("x").asThing().type().equals(specificType))
                            .collect(toSet()),
                    answersWithoutSpecifcTypeAndValue
            );

        }
    }

    //TODO need to introduce NegatedAtomicState for this to work with correct Omega
    @Ignore
    @Test
    public void negateSinglePattern(){
        try(Transaction tx = resourceAttachmentSession.transaction(Transaction.Type.READ)) {
            QueryBuilder qb = tx.graql().infer(true);

            String specificValue = "value";
            String attributeTypeLabel = "reattachable-resource-string";
            AttributeType attributeType = tx.getAttributeType(attributeTypeLabel);

            List<ConceptMap> fullAnswers = qb.<GetQuery>parse("match $x;get;").execute();

            List<ConceptMap> answers = qb.<GetQuery>parse(
                    "match " +
                            "NOT $x has " + attributeTypeLabel + " '" + specificValue + "';" +
                            "get;"
            ).execute();

            assertCollectionsEqual(
                    fullAnswers.stream()
                            .filter(ans -> !ans.get("r").asThing().attributes(attributeType).noneMatch(at -> at.value().equals(specificValue)))
                            .collect(toSet()),
                    answers
            );
        }

    }
}
