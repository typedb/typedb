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
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueries;
import grakn.core.graql.reasoner.query.ReasonerQueryEquivalence;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.graql.reasoner.utils.Pair;
import grakn.core.graql.reasoner.utils.ReasonerUtils;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static grakn.core.util.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
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
    public void taxfixMRE(){
        SessionImpl session = server.sessionWithNewKeyspace();

        //NB: loading data here as defining it as KB and using graql api leads to circular dependencies
        try(TransactionOLTP tx = session.transaction().write()) {
            tx.execute(Graql.parse("define " +
                    "someEntity sub entity," +
                    "has resource," +
                    "has anotherResource;" +
                    "resource sub attribute, datatype long;" +
                    "anotherResource sub attribute, datatype long;"
            ).asDefine());
            tx.commit();
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            String pattern = "{" +
                    "$x has resource $value;" +
                    "$x has anotherResource $anotherValue;" +
                    "$value == $anotherValue;" +
                    "$anotherValue > 0;" +
                    "};";
            ReasonerQueryImpl query = ReasonerQueries.create(conjunction(pattern), tx);
            System.out.println();
        }
    }

    private Conjunction<Statement> conjunction(String patternString){
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }

    @Test
    public void taxfixMRE2(){
        SessionImpl session = server.sessionWithNewKeyspace();
        try(TransactionOLTP tx = session.transaction().write()) {
            tx.execute(Graql.parse("define " +
                    "someEntity sub entity," +
                    "has derivedResource;" +
                    "derivedResource sub attribute, datatype long;" +
                    "rule1 sub rule, when{ $x isa someEntity;}, then { $x has derivedResource 1337;};" +
                    "rule2 sub rule, when{ $x isa someEntity;}, then { $x has derivedResource 1667;};"

            ).asDefine());
            tx.commit();
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            tx.execute(Graql.parse("insert " +
                    "$x isa someEntity;" +
                    "$y isa someEntity;"
            ).asInsert());
            tx.commit();
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            String pattern = "{" +
                    "$x has derivedResource $value;" +
                    "$y has derivedResource $anotherValue;" +
                    "$value == $anotherValue;" +
                    "$anotherValue > 1337;" +
                    "};";
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.parsePattern(pattern)).get());
            answers.forEach(ans -> {
                Object value = ans.get("value").asAttribute().value();
                Object anotherValue = ans.get("anotherValue").asAttribute().value();
                assertEquals(value, anotherValue);
                assertTrue((long) value > 1337L);
            });
            System.out.println();
        }
    }

    @Test
    public void whenParsingNeqValueVPS_equivalentPatternsMakeEquivalentQueries() {
        try(TransactionOLTP tx = attributeAttachmentSession.transaction().write()) {
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

            //if a comparison vp is inside attribute, we need to copy it outside as well to ensure correctness at the end of execution
            ResolvableQuery insideAttribute = ReasonerQueries.resolvable(neqInAttribute, tx);
            ResolvableQuery indirectOutside = ReasonerQueries.resolvable(indirectOutsideNeq, tx);
            assertTrue(ReasonerQueryEquivalence.AlphaEquivalence.equivalent(outsideAttribute, insideAttribute));
            assertTrue(ReasonerQueryEquivalence.AlphaEquivalence.equivalent(insideAttribute, indirectOutside));
        }
    }

    @Test
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void derivingResources_requireInequalityBetweenResources() {
        try(TransactionOLTP tx = attributeAttachmentSession.transaction().write()) {
            String neqVariant = "{ " +
                    "$x has derived-resource-string $value;" +
                    "$y has derived-resource-string $anotherValue;" +
                    "$value !== $anotherValue;" +
                    "};";

            String neqVariantWithExtraCondition = "{ " +
                    "$x has derived-resource-string $value;" +
                    "$y has derived-resource-string $anotherValue;" +
                    "$value !== $anotherValue;" +
                    "$anotherValue contains 'value';" +
                    "};";

            String eqVariant = "{ " +
                    "$x has derived-resource-string $value;" +
                    "$y has derived-resource-string $anotherValue;" +
                    "$value == $anotherValue;" +
                    "};";

            String eqVariantWithExtraCondition = "{ " +
                    "$x has derived-resource-string $value;" +
                    "$y has derived-resource-string $anotherValue;" +
                    "$value == $anotherValue;" +
                    "$anotherValue contains 'value';" +
                    "};";

            ReasonerQueryImpl query = ReasonerQueries.create(conjunction(neqVariant), tx);
            ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(neqVariantWithExtraCondition), tx);
            ReasonerQueryImpl query3 = ReasonerQueries.create(conjunction(eqVariant), tx);
            ReasonerQueryImpl query4 = ReasonerQueries.create(conjunction(eqVariantWithExtraCondition), tx);
            List<ConceptMap> neqAnswers = tx.execute(Graql.match(Graql.parsePattern(neqVariant)).get());
            List<ConceptMap> neqAnswersWithCondition = tx.execute(Graql.match(Graql.parsePattern(neqVariantWithExtraCondition)).get());
            List<ConceptMap> eqAnswers = tx.execute(Graql.match(Graql.parsePattern(eqVariant)).get());
            List<ConceptMap> eqAnswersWithCondition = tx.execute(Graql.match(Graql.parsePattern(eqVariantWithExtraCondition)).get());

            neqAnswers.forEach(ans -> {
                Object value = ans.get("value").asAttribute().value();
                Object anotherValue = ans.get("anotherValue").asAttribute().value();
                assertNotEquals(value, anotherValue);
            });
        }
    }

    @Test
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void derivingResources_requireNotHavingSpecificValue() {
        try(TransactionOLTP tx = attributeAttachmentSession.transaction().write()) {
            String neqVariant = "{ " +
                    "$x has derived-resource-string $val;$val !== 'unattached';" +
                    "};";
            String neqVariant2 = "{ " +
                    "$x has derived-resource-string $val;" +
                    "$unwanted == 'unattached';" +
                    "$val !== $unwanted;" +
                    "};";
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

            ReasonerAtomicQuery query = ReasonerQueries.atomic(conjunction(neqVariant), tx);
            ReasonerAtomicQuery query2 = ReasonerQueries.atomic(conjunction(neqVariant2), tx);

            String complementQueryString = "match $x has derived-resource-string $val; $val == 'unattached'; get;";
            String completeQueryString = "match $x has derived-resource-string $val; get;";

            List<ConceptMap> answers = tx.execute(Graql.match(Graql.parsePattern(neqVariant)).get());

            List<ConceptMap> answersBis = tx.execute(Graql.match(Graql.parsePattern(neqVariant2)).get());
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
        try(TransactionOLTP tx = attributeAttachmentSession.transaction().write()) {
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
        try(TransactionOLTP tx = attributeAttachmentSession.transaction().write()) {
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
        try(TransactionOLTP tx = attributeAttachmentSession.transaction().write()) {
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
