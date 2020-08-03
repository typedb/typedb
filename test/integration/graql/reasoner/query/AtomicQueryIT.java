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
 *
 */

package grakn.core.graql.reasoner.query;

import grakn.core.common.config.Config;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.test.common.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class AtomicQueryIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session session;

    @BeforeClass
    public static void loadContext(){
        Config mockServerConfig = storage.createCompatibleServerConfig();
        session = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            // define schema
            tx.execute(Graql.parse("define " +
                    "ownership sub relation, " +
                    "  relates owner," +
                    "  relates owned;" +
                    "borrowing sub ownership," +
                    "  relates borrower as owner," +
                    "  relates borrowed as owned;" +
                    "friendship sub relation," +
                    "  relates friend;" +
                    "best-friendship sub relation," +
                    "  relates best-friend as friend;" +
                    "person sub entity," +
                    "  plays owner," +
                    "  plays friend, " +
                    "  plays best-friend," +
                    "  has name;" +
                    "reader sub person, " +
                    "  plays borrower;" +
                    "item sub entity," +
                    "  plays owned;" +
                    "book sub item," +
                    "  plays borrowed;" +
                    "name sub attribute, value string;").asDefine());
            tx.commit();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenConstructingNonAtomicQuery_ExceptionIsThrown() {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
            String patternString = "{ ($x, $y) isa friendship;($y, $z) isa friendship; };";
            ReasonerAtomicQuery atomicQuery = reasonerQueryFactory.atomic(conjunction(patternString));
        }
    }

    @Test(expected = GraqlSemanticException.class)
    public void whenCreatingQueryWithNonexistentType_ExceptionIsThrown() {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String patternString = "{ $x isa space-tractor; };";
            ReasonerAtomicQuery query = reasonerQueryFactory.atomic(conjunction(patternString));
        }
    }

    @Test(expected = GraknConceptException.class)
    public void whenCreatingAttributeQueryWithInvalidValueType_ExceptionIsThrown() {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String patternString = "{ $x has name 100.01; };";
            ReasonerAtomicQuery query = reasonerQueryFactory.atomic(conjunction(patternString));
        }
    }

    @Test
    public void whenCopyingQuery_TheCopyIsAlphaEquivalentToOriginal() {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String patternString = "{ $x isa person;$y isa reader;($x, $y) isa borrowing; };";
            Conjunction<Statement> pattern = conjunction(patternString);
            ReasonerAtomicQuery atomicQuery = reasonerQueryFactory.atomic(pattern);
            ReasonerAtomicQuery copy = atomicQuery.copy();
            assertEquals(atomicQuery, copy);
            assertEquals(atomicQuery.hashCode(), copy.hashCode());
        }
    }

    @Test
    public void whenQueryingForRelationsWithAmbiguousRoleTypes_answersArePermutedCorrectly() {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String childString = "match (borrowed: $x, borrower: $y) isa borrowing; get;";
            String parentString = "match ($x, $y) isa borrowing; get;";

            GraqlGet childQuery = Graql.parse(childString).asGet();
            GraqlGet parentQuery = Graql.parse(parentString).asGet();
            Set<ConceptMap> answers = tx.stream(childQuery, false).collect(toSet());
            Set<ConceptMap> fullAnswers = tx.stream(parentQuery, false).collect(toSet());
            Atom childAtom = reasonerQueryFactory.atomic(conjunction(childQuery.match().getPatterns())).getAtom();
            Atom parentAtom = reasonerQueryFactory.atomic(conjunction(parentQuery.match().getPatterns())).getAtom();

            MultiUnifier multiUnifier = childAtom.getMultiUnifier(childAtom, UnifierType.RULE);
            Set<ConceptMap> permutedAnswers = answers.stream()
                    .flatMap(multiUnifier::apply)
                    .collect(Collectors.toSet());

            MultiUnifier multiUnifier2 = childAtom.getMultiUnifier(parentAtom, UnifierType.RULE);
            Set<ConceptMap> permutedAnswers2 = answers.stream()
                    .flatMap(multiUnifier2::apply)
                    .collect(Collectors.toSet());

            assertEquals(fullAnswers, permutedAnswers2);
            assertEquals(answers, permutedAnswers);
        }
    }

    private Conjunction<Statement> conjunction(String patternString) {
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }

    private Conjunction<Statement> conjunction(Conjunction<Pattern> pattern) {
        Set<Statement> vars = pattern
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }

}
