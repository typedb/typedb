/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
import grakn.core.concept.Label;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.type.Type;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.predicate.VariableValuePredicate;
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
import java.util.AbstractCollection;
import java.util.List;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static grakn.core.util.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

public class ValuePredicateIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl attributeAttachmentSession;
    @BeforeClass
    public static void loadContext(){
        attributeAttachmentSession = server.sessionWithNewKeyspace();
        String resourcePath = "test-integration/graql/reasoner/stubs/";
        loadFromFileAndCommit(resourcePath, "resourceAttachment.gql", attributeAttachmentSession);
    }

    @AfterClass
    public static void closeSession(){
        attributeAttachmentSession.close();
    }

    @Test
    public void whenResolvingInferrableAttributesWithBounds_answersAreCalculatedCorrectly(){
        SessionImpl session = server.sessionWithNewKeyspace();
        try(TransactionOLTP tx = session.transaction().write()) {
            tx.execute(Graql.parse("define " +
                    "someEntity sub entity," +
                    "has derivedResource;" +
                    "derivedResource sub attribute, datatype long;" +
                    "rule1 sub rule, when{ $x isa someEntity;}, then { $x has derivedResource 1337;};" +
                    "rule2 sub rule, when{ $x isa someEntity;}, then { $x has derivedResource 1667;};" +
                    "rule3 sub rule, when{ $x isa someEntity;}, then { $x has derivedResource 1997;};"

            ).asDefine());
            tx.execute(Graql.parse("insert " +
                    "$x isa someEntity;" +
                    "$y isa someEntity;"
            ).asInsert());
            tx.commit();
        }

        final long bound = 1667L;
        Statement value = Graql.var("value");
        Pattern basePattern = Graql.var("x").has("derivedResource", value);

        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, value.gt(bound))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> assertTrue((long) ans.get(value.var()).asAttribute().value() > bound));
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, value.gte(bound))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> assertTrue((long) ans.get(value.var()).asAttribute().value() >= bound));
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, value.lt(bound))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> assertTrue((long) ans.get(value.var()).asAttribute().value() < bound));
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, value.lte(bound))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> assertTrue((long) ans.get(value.var()).asAttribute().value() <= bound));
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, value.eq(bound))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> assertTrue((long) ans.get(value.var()).asAttribute().value() == bound));
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, value.neq(bound))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> assertTrue((long) ans.get(value.var()).asAttribute().value() != bound));
        }
    }

    @Test
    public void whenResolvableAttributesHaveVariableComparisons_answersAreCalculatedCorrectly(){
        SessionImpl session = server.sessionWithNewKeyspace();
        try(TransactionOLTP tx = session.transaction().write()) {
            tx.execute(Graql.parse("define " +
                    "someEntity sub entity," +
                    "has derivedResource;" +
                    "derivedResource sub attribute, datatype long;" +
                    "rule1 sub rule, when{ $x isa someEntity;}, then { $x has derivedResource 1337;};" +
                    "rule2 sub rule, when{ $x isa someEntity;}, then { $x has derivedResource 1667;};"

            ).asDefine());
            tx.execute(Graql.parse("insert " +
                    "$x isa someEntity;" +
                    "$y isa someEntity;"
            ).asInsert());
            tx.commit();
        }

        Statement value = Graql.var("value");
        Statement anotherValue = Graql.var("anotherValue");
        Pattern basePattern = Graql.and(
                Graql.var("x").has("derivedResource", value),
                Graql.var("y").has("derivedResource", anotherValue)
        );

        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, value.gt(anotherValue))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> {
                assertTrue((long) ans.get(value.var()).asAttribute().value() > (long) ans.get(anotherValue.var()).asAttribute().value());
            });
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, value.gte(anotherValue))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> {
                assertTrue((long) ans.get(value.var()).asAttribute().value() >= (long) ans.get(anotherValue.var()).asAttribute().value());
            });
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, value.lt(anotherValue))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> {
                assertTrue((long) ans.get(value.var()).asAttribute().value() < (long) ans.get(anotherValue.var()).asAttribute().value());
            });
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, value.lte(anotherValue))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> {
                assertTrue((long) ans.get(value.var()).asAttribute().value() <= (long) ans.get(anotherValue.var()).asAttribute().value());
            });
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, value.eq(anotherValue))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> {
                assertEquals((long) ans.get(value.var()).asAttribute().value(), (long) ans.get(anotherValue.var()).asAttribute().value());
            });
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, value.neq(anotherValue))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> {
                assertTrue((long) ans.get(value.var()).asAttribute().value() != (long) ans.get(anotherValue.var()).asAttribute().value());
            });
        }
    }

    @Test
    public void whenResolvableAttributesHaveVariableComparisonsWithABound_answersAreCalculatedCorrectly(){
        SessionImpl session = server.sessionWithNewKeyspace();
        try(TransactionOLTP tx = session.transaction().write()) {
            tx.execute(Graql.parse("define " +
                    "someEntity sub entity," +
                    "has derivedResource;" +
                    "derivedResource sub attribute, datatype long;" +
                    "rule1 sub rule, when{ $x isa someEntity;}, then { $x has derivedResource 1337;};" +
                    "rule2 sub rule, when{ $x isa someEntity;}, then { $x has derivedResource 1667;};" +
                    "rule3 sub rule, when{ $x isa someEntity;}, then { $x has derivedResource 1997;};"

            ).asDefine());
            tx.execute(Graql.parse("insert " +
                    "$x isa someEntity;" +
                    "$y isa someEntity;"
            ).asInsert());
            tx.commit();
        }

        final long bound = 1667L;
        Statement value = Graql.var("value");
        Statement anotherValue = Graql.var("anotherValue");
        Pattern basePattern = Graql.and(
                Graql.var("x").has("derivedResource", value),
                Graql.var("y").has("derivedResource", anotherValue),
                value.eq(anotherValue)
        );

        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, anotherValue.gt(bound))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> assertTrue((long) ans.get(value.var()).asAttribute().value() > bound));
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, anotherValue.gte(bound))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> assertTrue((long) ans.get(value.var()).asAttribute().value() >= bound));
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, anotherValue.lt(bound))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> assertTrue((long) ans.get(value.var()).asAttribute().value() < bound));
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, anotherValue.lte(bound))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> assertTrue((long) ans.get(value.var()).asAttribute().value() <= bound));
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, anotherValue.eq(bound))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> assertTrue((long) ans.get(value.var()).asAttribute().value() == bound));
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<ConceptMap> answers = tx.execute(Graql.match(Graql.and(basePattern, anotherValue.neq(bound))).get());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> assertTrue((long) ans.get(value.var()).asAttribute().value() != bound));
        }
    }

    @Test
    public void whenParsingNeqValueVPs_ensureTheyAreParsedIntoAtomsCorrectly() {
        try(TransactionOLTP tx = attributeAttachmentSession.transaction().write()) {
            Conjunction<Pattern> neqWithoutBound = Iterables.getOnlyElement(
                    Graql.parsePattern(
                            "{" +
                                    "$x has derived-resource-string $val;" +
                                    "$y has derived-resource-string $anotherVal;" +
                                    "$val !== $anotherVal;" +
                                    "};"
                    ).getNegationDNF().getPatterns()
            );
            Conjunction<Pattern> neqWithoutBound2 = Iterables.getOnlyElement(
                    Graql.parsePattern(
                            "{" +
                                    "$x has derived-resource-string !== $anotherVal;" +
                                    "$y has derived-resource-string $anotherVal;" +
                                    "};"
                    ).getNegationDNF().getPatterns()
            );
            Conjunction<Pattern> neqWithSoftBound = Iterables.getOnlyElement(
                    Graql.parsePattern(
                            "{" +
                                    "$x has derived-resource-string $val;" +
                                    "$y has derived-resource-string $anotherVal;" +
                                    "$val !== $anotherVal;" +
                                    "$anotherVal > 'value';" +
                                    "};"
                    ).getNegationDNF().getPatterns()
            );
            Conjunction<Pattern> neqWithBound = Iterables.getOnlyElement(
                    Graql.parsePattern(
                            "{" +
                                    "$x has derived-resource-string $val;" +
                                    "$y has derived-resource-string $anotherVal;" +
                                    "$val !== $anotherVal;" +
                                    "$anotherVal == 'value';" +
                                    "};"
                    ).getNegationDNF().getPatterns()
            );

            Conjunction<Pattern> negationWithIndirectBound = Iterables.getOnlyElement(
                    Graql.parsePattern(
                            "{ " +
                                    "$x has derived-resource-string $val;" +
                                    "not {$val == $unwanted;$unwanted == 'unattached';};" +
                                    "};"
                    ).getNegationDNF().getPatterns()
            );

            Conjunction<Pattern> negationWithBound = Iterables.getOnlyElement(
                    Graql.parsePattern(
                            "{ " +
                                    "$x has derived-resource-string $val;" +
                                    "not {$val == 'unattached';};" +
                                    "};"
                    ).getNegationDNF().getPatterns()
            );

            ResolvableQuery unboundNeqQuery = ReasonerQueries.resolvable(neqWithoutBound, tx);
            ResolvableQuery unboundNeqQuery2 = ReasonerQueries.resolvable(neqWithoutBound2, tx);
            ResolvableQuery boundNeqQuery = ReasonerQueries.resolvable(neqWithBound, tx);
            ResolvableQuery softBoundNeqQuery = ReasonerQueries.resolvable(neqWithSoftBound, tx);
            ResolvableQuery negatedQueryWithIndirectBound = ReasonerQueries.resolvable(negationWithIndirectBound, tx);
            ResolvableQuery negatedQueryWithBound = ReasonerQueries.resolvable(negationWithBound, tx);

            //we keep unbound predicates outside attributes
            assertTrue(ReasonerQueryEquivalence.AlphaEquivalence.equivalent(unboundNeqQuery, unboundNeqQuery2));
            assertTrue(unboundNeqQuery.getAtoms(AttributeAtom.class).map(AttributeAtom::getMultiPredicate).allMatch(AbstractCollection::isEmpty));
            assertTrue(unboundNeqQuery.getAtoms(VariableValuePredicate.class).findFirst().isPresent());

            //if the predicate has a soft bound, we try to incorporate it into relevant attribute
            //we still keep the variable predicate outside
            assertTrue(softBoundNeqQuery.getAtoms(AttributeAtom.class).map(AttributeAtom::getMultiPredicate).anyMatch(AbstractCollection::isEmpty));
            assertTrue(softBoundNeqQuery.getAtoms(AttributeAtom.class).map(AttributeAtom::getMultiPredicate).anyMatch(pred -> !pred.isEmpty()));
            assertTrue(softBoundNeqQuery.getAtoms(VariableValuePredicate.class).findFirst().isPresent());

            //if the predicate has a hard bound, we try to incorporate it into relevant attribute
            //we still keep the variable predicate outside
            assertTrue(boundNeqQuery.getAtoms(AttributeAtom.class).map(AttributeAtom::getMultiPredicate).anyMatch(AbstractCollection::isEmpty));
            assertTrue(boundNeqQuery.getAtoms(AttributeAtom.class).map(AttributeAtom::getMultiPredicate).anyMatch(pred -> !pred.isEmpty()));
            assertTrue(boundNeqQuery.getAtoms(VariableValuePredicate.class).findFirst().isPresent());

            assertTrue(ReasonerQueryEquivalence.AlphaEquivalence.equivalent(negatedQueryWithBound, negatedQueryWithIndirectBound));
        }
    }

    @Test
    public void whenParsingNeqValueVPsWithValue_equivalentPatternsMakeEquivalentQueries() {
        try(TransactionOLTP tx = attributeAttachmentSession.transaction().write()) {
            Conjunction<Pattern> neqOutsideAttribute = Iterables.getOnlyElement(Graql.parsePattern(
                    "{" +
                            "$x has derived-resource-string $val;$val !== 'unattached';" +
                    "};").getNegationDNF().getPatterns()
            );
            Conjunction<Pattern> neqInsideAttribute = Iterables.getOnlyElement(Graql.parsePattern(
                    "{"+
                            "$x has derived-resource-string !== 'unattached';" +
                            "};").getNegationDNF().getPatterns()
            );
            Conjunction<Pattern> indirectOutsideNeq = Iterables.getOnlyElement(Graql.parsePattern(
                    "{" +
                            "$x has derived-resource-string $val;" +
                            "$unwanted == 'unattached';" +
                            "$val !== $unwanted;" +
                    "};").getNegationDNF().getPatterns());

            ResolvableQuery outsideAttribute = ReasonerQueries.resolvable(neqOutsideAttribute, tx);

            //if a comparison vp is inside attribute, we need to copy it outside as well to ensure correctness at the end of execution
            ResolvableQuery insideAttribute = ReasonerQueries.resolvable(neqInsideAttribute, tx);
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

            neqAnswersWithCondition.forEach(ans -> {
                Object value = ans.get("value").asAttribute().value();
                Object anotherValue = ans.get("anotherValue").asAttribute().value();
                assertNotEquals(value, anotherValue);
                assertTrue(anotherValue.toString().contains("value"));
            });

            eqAnswers.forEach(ans -> {
                Object value = ans.get("value").asAttribute().value();
                Object anotherValue = ans.get("anotherValue").asAttribute().value();
                assertEquals(value, anotherValue);
            });

            eqAnswersWithCondition.forEach(ans -> {
                Object value = ans.get("value").asAttribute().value();
                Object anotherValue = ans.get("anotherValue").asAttribute().value();
                assertEquals(value, anotherValue);
                assertTrue(anotherValue.toString().contains("value"));
            });
        }
    }

    @Test
    //Expected result: When the head of a rule contains resource assertions, the respective unique resources should be generated or reused.
    public void derivingResources_requireNotHavingSpecificValue() {
        try(TransactionOLTP tx = attributeAttachmentSession.transaction().write()) {
            String neqHardBound = "{ " +
                    "$x has derived-resource-string $val;$val !== 'unattached';" +
                    "};";
            String neqHardBound2 = "{ " +
                    "$x has derived-resource-string $val;" +
                    "$unwanted == 'unattached';" +
                    "$val !== $unwanted;" +
                    "};";
            String negatedVpHardBound = "{ " +
                    "$x has derived-resource-string $val;" +
                    "not {$val == 'unattached';};" +
                    "};";
            String negatedVpHardBound2 = "{ " +
                    "$x has derived-resource-string $val;" +
                    "not {$val == $unwanted;$unwanted == 'unattached';};" +
                    "};";
            String negatedNeqVpHardBound = "{ " +
                    "$x has derived-resource-string $val;" +
                    "not {$val !== 'unattached';};" +
                    "};";

            ReasonerAtomicQuery neqHardBoundQuery = ReasonerQueries.atomic(conjunction(neqHardBound), tx);
            ReasonerAtomicQuery neqHardBoundQuery2 = ReasonerQueries.atomic(conjunction(neqHardBound2), tx);
            ResolvableQuery negatedVpHardBoundQuery = ReasonerQueries.resolvable(conjunctionWithNegation(negatedVpHardBound), tx);
            ResolvableQuery negatedVpHardBoundQuery2 = ReasonerQueries.resolvable(conjunctionWithNegation(negatedVpHardBound2), tx);
            ResolvableQuery negatedNeqVpHardBoundQuery = ReasonerQueries.resolvable(conjunctionWithNegation(negatedNeqVpHardBound), tx);

            assertTrue(ReasonerQueryEquivalence.AlphaEquivalence.equivalent(neqHardBoundQuery, neqHardBoundQuery2));
            assertTrue(ReasonerQueryEquivalence.AlphaEquivalence.equivalent(negatedVpHardBoundQuery, negatedVpHardBoundQuery2));

            String complementQueryPattern = "{$x has derived-resource-string $val; $val == 'unattached';};";
            String completeQueryPattern = "{$x has derived-resource-string $val;};";

            ReasonerAtomicQuery complementQuery = ReasonerQueries.atomic(conjunction(complementQueryPattern), tx);

            List<ConceptMap> answers = tx.execute(Graql.match(Graql.parsePattern(neqHardBound)).get());

            List<ConceptMap> answersBis = tx.execute(Graql.match(Graql.parsePattern(neqHardBound2)).get());
            List<ConceptMap> negationAnswers = tx.execute(Graql.match(Graql.parsePattern(negatedVpHardBound)).get());
            List<ConceptMap> negationAnswersBis = tx.execute(Graql.match(Graql.parsePattern(negatedVpHardBound2)).get());
            List<ConceptMap> negationComplement = tx.execute(Graql.match(Graql.parsePattern(negatedNeqVpHardBound)).get());

            List<ConceptMap> complement = tx.execute(Graql.match(Graql.parsePattern(complementQueryPattern)).get());
            List<ConceptMap> complete = tx.execute(Graql.match(Graql.parsePattern(completeQueryPattern)).get());
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

    @Test
    public void derivingResources_requireAnEntityToHaveTwoDistinctResourcesOfNotAbstractType() {
        try(TransactionOLTP tx = attributeAttachmentSession.transaction().write()) {
            String queryString = "match " +
                    "$x has derivable-resource-string $value;" +
                    "$x has derivable-resource-string $unwantedValue;" +
                    "$value !== $unwantedValue;$unwantedValue 'unattached';" +
                    "$value isa $type;" +
                    "$unwantedValue isa $type;" +
                    "$type != $unwantedType;$unwantedType type derivable-resource-string;" +
                    "get;";
            GraqlGet query = Graql.parse(queryString).asGet();
            List<ConceptMap> answers = tx.execute(query);
            Type unwantedType = tx.getType(Label.of("derivable-resource-string"));
            answers.forEach(ans -> {
                Type type = ans.get("type").asType();
                Object value = ans.get("value").asAttribute().value();
                assertNotEquals("unattached", value);
                assertNotEquals(unwantedType, type);
            });
        }
    }

    private Conjunction<Statement> conjunction(String patternString){
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }

    private Conjunction<Pattern> conjunctionWithNegation(String patternString){
        Set<Pattern> vars = Graql.parsePattern(patternString)
                .getNegationDNF().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}
