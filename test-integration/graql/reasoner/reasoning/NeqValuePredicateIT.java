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

import com.google.common.collect.Iterables;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.query.ReasonerQueries;
import grakn.core.graql.reasoner.query.ReasonerQueryEquivalence;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.graql.reasoner.utils.Pair;
import grakn.core.graql.reasoner.utils.ReasonerUtils;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlGet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static grakn.core.util.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static junit.framework.TestCase.assertTrue;
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
    public void whenParsingNeqValueVPS_equivalentPatternsMakeEquivalentQueries() {
        try(TransactionOLTP tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
            Conjunction<Pattern> neqOutsideAttribute = Iterables.getOnlyElement(
                    Graql.parsePattern("{$x has derived-resource-string $val;$val !== 'unattached';};").getNegationDNF().getPatterns()
            );
            Conjunction<Pattern> neqInAttribute = Iterables.getOnlyElement(
                    Graql.parsePattern("{$x has derived-resource-string !== 'unattached';};").getNegationDNF().getPatterns()
            );
            Conjunction<Pattern> indirectOutsideNeq = Iterables.getOnlyElement(Graql.parsePattern("{" +
                    "$x has derived-resource-string $val;" +
                    "$unwanted == 'unattached';" +
                    "$val !== $unwanted;" +
                    "};").getNegationDNF().getPatterns());
            ResolvableQuery outsideAttribute = ReasonerQueries.resolvable(neqOutsideAttribute, tx);
            ResolvableQuery insideAttribute = ReasonerQueries.resolvable(neqInAttribute, tx);
            ResolvableQuery indirectOutside = ReasonerQueries.resolvable(indirectOutsideNeq, tx);
            assertTrue(ReasonerQueryEquivalence.AlphaEquivalence.equivalent(outsideAttribute, insideAttribute));
            assertTrue(ReasonerQueryEquivalence.AlphaEquivalence.equivalent(insideAttribute, indirectOutside));
        }
    }

    @Test
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void derivingResources_requireNotHavingSpecificValue() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
            String neqVariant = "match " +
                    "$x has derived-resource-string $val;$val !== 'unattached';" +
                    "get;";
            String neqVariant2 = "match " +
                    "$x has derived-resource-string $val;" +
                    "$unwanted == 'unattached';" +
                    "$val !== $unwanted; get;";
            String negatedVariant = "match " +
                    "$x has derived-resource-string $val;" +
                    "not {$val == 'unattached';};" +
                    "get;";
            String negatedVariant2 = "match " +
                    "$x has derived-resource-string $val;" +
                    "not {$val == $unwanted;$unwanted == 'unattached';};" +
                    "get;";
            String negatedComplement = "match " +
                    "$x has derived-resource-string $val;" +
                    "not {$val !== 'unattached';};" +
                    "get;";

            String complementQueryString = "match $x has derived-resource-string $val; $val == 'unattached'; get;";
            String completeQueryString = "match $x has derived-resource-string $val; get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(neqVariant).asGet());
            List<ConceptMap> answersBis = tx.execute(Graql.parse(neqVariant2).asGet());
            List<ConceptMap> negationAnswers = tx.execute(Graql.parse(negatedVariant).asGet());
            List<ConceptMap> negationAnswersBis = tx.execute(Graql.parse(negatedVariant2).asGet());
            List<ConceptMap> negationComplement = tx.execute(Graql.parse(negatedComplement).asGet());

            List<ConceptMap> complement = tx.execute(Graql.parse(complementQueryString).asGet());
            List<ConceptMap> complete = tx.execute(Graql.parse(completeQueryString).asGet());
            List<ConceptMap> expectedAnswers = ReasonerUtils.listDifference(complete, complement);

            assertCollectionsNonTriviallyEqual(expectedAnswers, answers);
            assertCollectionsNonTriviallyEqual(answers, negationAnswers);
            assertCollectionsNonTriviallyEqual(complement, negationComplement);
            assertCollectionsNonTriviallyEqual(expectedAnswers, answersBis);
            assertCollectionsNonTriviallyEqual(answersBis, negationAnswersBis);
        }
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void derivingResources_requireResourceValuesToBeDifferent() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
            String neqVersion = "match " +
                    "$x has derived-resource-string $val;" +
                    "$y has reattachable-resource-string $anotherVal;" +
                    "$val !== $anotherVal;" +
                    "get;";
            String negationVersion = "match " +
                    "$x has derived-resource-string $val;" +
                    "$y has reattachable-resource-string $anotherVal;" +
                    "not {$val == $anotherVal;};" +
                    "get;";
            List<ConceptMap> answers = tx.execute(Graql.parse(neqVersion).asGet());
            answers.stream()
                    .map(ans -> new Pair<>(ans.get("val").asAttribute().value(), ans.get("anotherVal").asAttribute().value()))
                    .forEach(p -> assertNotEquals(p.getKey(), p.getValue()));
            List<ConceptMap> negationAnswers = tx.execute(Graql.parse(negationVersion).asGet());
            assertCollectionsNonTriviallyEqual(answers, negationAnswers);

        }
    }

    @Test //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void derivingResources_requireInstanceValuesToBeDifferent() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
            String neqVersion = "match " +
                    "$val isa derived-resource-string;" +
                    "$anotherVal isa reattachable-resource-string;" +
                    "$val !== $anotherVal;" +
                    "get;";
            String negationVersion = "match " +
                    "$val isa derived-resource-string;" +
                    "$anotherVal isa reattachable-resource-string;" +
                    "not {$val == $anotherVal;};" +
                    "get;";

            List<ConceptMap> answers = tx.execute(Graql.parse(neqVersion).asGet());
            answers.stream()
                    .map(ans -> new Pair<>(ans.get("val").asAttribute().value(), ans.get("anotherVal").asAttribute().value()))
                    .forEach(p -> assertNotEquals(p.getKey(), p.getValue()));
            List<ConceptMap> negationAnswers = tx.execute(Graql.parse(negationVersion).asGet());
            assertCollectionsNonTriviallyEqual(answers, negationAnswers);
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
            GraqlGet query = Graql.parse(queryString).asGet();
            tx.execute(query);
        }
    }
}
