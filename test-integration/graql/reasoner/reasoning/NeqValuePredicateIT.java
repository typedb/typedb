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

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.utils.ReasonerUtils;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.Graql;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import static grakn.core.util.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static org.junit.Assert.assertNotEquals;

public class NeqValuePredicateIT {

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
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void derivingResources_requireNotHavingSpecificValue() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
            String queryString = "match " +
                    "$x has derived-resource-string $val;$val !== 'unattached';" +
                    "get;";
            String queryString2 = "match " +
                    "$x has derived-resource-string $val;" +
                    "$unwanted == 'unattached';" +
                    "$val !== $unwanted; get;";

            String complementQueryString = "match $x has derived-resource-string $val; $val == 'unattached'; get;";
            String completeQueryString = "match $x has derived-resource-string $val; get;";

            List<ConceptMap> answers = tx.execute(Graql.<GetQuery>parse(queryString));
            List<ConceptMap> answersBis = tx.execute(Graql.<GetQuery>parse(queryString2));


            List<ConceptMap> complement = tx.execute(Graql.<GetQuery>parse(complementQueryString));
            List<ConceptMap> complete = tx.execute(Graql.<GetQuery>parse(completeQueryString));
            List<ConceptMap> expectedAnswers = ReasonerUtils.listDifference(complete, complement);

            assertCollectionsNonTriviallyEqual(expectedAnswers, answers);
            assertCollectionsNonTriviallyEqual(expectedAnswers, answersBis);

        }
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void derivingResources_requireValuesToBeDifferent() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
            String queryString = "match " +
                    "$x has derived-resource-string $val;" +
                    "$y has reattachable-resource-string $anotherVal;" +
                    "$val !== $anotherVal;" +
                    "get;";

            tx.stream(Graql.<GetQuery>parse(queryString)).forEach(ans -> assertNotEquals(ans.get("val"), ans.get("anotherVal")));
        }
    }

    //TODO another bug here
    @Ignore
    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void derivingResources_requireAnEntityToHaveTwoDistinctResourcesOfNotAbstractType() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
            String queryString = "match " +
                    "$x has derivable-resource-string $value;" +
                    "$x has derivable-resource-string $unwantedValue;" +
                    "$unwantedValue 'unattached';" +
                    "$value !== $unwantedValue;" +
                    "$value isa $type;" +
                    "$unwantedValue isa $type;" +
                    "$type != $unwantedType;" +
                    "$unwantedType type 'derivable-resource-string';" +
                    "get;";
            GetQuery query = Graql.parse(queryString);
            List<ConceptMap> execute = tx.execute(query);
            System.out.println();
        }
    }
}
