/*
 * Copyright (C) 2020 Grakn Labs
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
import grakn.common.util.Pair;
import grakn.core.common.config.Config;
import grakn.core.common.util.ListsUtil;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.predicate.VariableValuePredicate;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryEquivalence;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestStorage;
import grakn.core.test.rule.SessionUtil;
import grakn.core.test.rule.TestTransactionProvider;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.AbstractCollection;
import java.util.List;
import java.util.Set;

import static grakn.core.test.common.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static grakn.core.test.common.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

public class ValuePredicateIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session attributeAttachmentSession;
    @BeforeClass
    public static void loadContext(){
        Config mockServerConfig = storage.createCompatibleServerConfig();
        attributeAttachmentSession = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        String resourcePath = "test/integration/graql/reasoner/stubs/";
        loadFromFileAndCommit(resourcePath, "resourceAttachment.gql", attributeAttachmentSession);
    }

    @AfterClass
    public static void closeSession(){
        attributeAttachmentSession.close();
    }

    @Test
    public void whenParsingNeqValueVPs_ensureTheyAreParsedIntoAtomsCorrectly() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
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

            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            ResolvableQuery unboundNeqQuery = reasonerQueryFactory.resolvable(neqWithoutBound);
            ResolvableQuery unboundNeqQuery2 = reasonerQueryFactory.resolvable(neqWithoutBound2);
            ResolvableQuery boundNeqQuery = reasonerQueryFactory.resolvable(neqWithBound);
            ResolvableQuery softBoundNeqQuery = reasonerQueryFactory.resolvable(neqWithSoftBound);
            ResolvableQuery negatedQueryWithIndirectBound = reasonerQueryFactory.resolvable(negationWithIndirectBound);
            ResolvableQuery negatedQueryWithBound = reasonerQueryFactory.resolvable(negationWithBound);

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
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
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

            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            ResolvableQuery outsideAttribute = reasonerQueryFactory.resolvable(neqOutsideAttribute);

            //if a comparison vp is inside attribute, we need to copy it outside as well to ensure correctness at the end of execution
            ResolvableQuery insideAttribute = reasonerQueryFactory.resolvable(neqInsideAttribute);
            ResolvableQuery indirectOutside = reasonerQueryFactory.resolvable(indirectOutsideNeq);
            assertTrue(ReasonerQueryEquivalence.AlphaEquivalence.equivalent(outsideAttribute, insideAttribute));
            assertTrue(ReasonerQueryEquivalence.AlphaEquivalence.equivalent(insideAttribute, indirectOutside));
        }
    }

    @Test
    // Expected result: Negation of !== should be considered equivalent to ==, and vice versa
    public void derivingResources_assertEquivalenceOfNegations() {
        try(Transaction tx = attributeAttachmentSession.transaction(Transaction.Type.WRITE)) {
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

            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            ReasonerAtomicQuery neqHardBoundQuery = reasonerQueryFactory.atomic(conjunction(neqHardBound));
            ReasonerAtomicQuery neqHardBoundQuery2 = reasonerQueryFactory.atomic(conjunction(neqHardBound2));
            ResolvableQuery negatedVpHardBoundQuery = reasonerQueryFactory.resolvable(conjunctionWithNegation(negatedVpHardBound));
            ResolvableQuery negatedVpHardBoundQuery2 = reasonerQueryFactory.resolvable(conjunctionWithNegation(negatedVpHardBound2));

            assertTrue(ReasonerQueryEquivalence.AlphaEquivalence.equivalent(neqHardBoundQuery, neqHardBoundQuery2));
            assertTrue(ReasonerQueryEquivalence.AlphaEquivalence.equivalent(negatedVpHardBoundQuery, negatedVpHardBoundQuery2));
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
