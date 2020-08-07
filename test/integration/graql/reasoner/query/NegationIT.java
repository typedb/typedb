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

import com.google.common.collect.Iterables;
import grakn.core.common.config.Config;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.reasoner.ReasonerException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestStorage;
import grakn.core.test.rule.SessionUtil;
import grakn.core.test.rule.TestTransactionProvider;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Negation;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlGet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static grakn.core.test.common.GraqlTestUtil.loadFromFileAndCommit;
import static graql.lang.Graql.and;
import static graql.lang.Graql.match;
import static graql.lang.Graql.not;
import static graql.lang.Graql.or;
import static graql.lang.Graql.var;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertNotEquals;

public class NegationIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session negationSession;

    @BeforeClass
    public static void loadContext(){
        Config mockServerConfig = storage.createCompatibleServerConfig();
        negationSession = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        String resourcePath = "test/integration/graql/reasoner/stubs/";
        loadFromFileAndCommit(resourcePath,"negation.gql", negationSession);
    }

    @AfterClass
    public static void closeSession(){
        negationSession.close();
    }

    @org.junit.Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test (expected = ReasonerException.class)
    public void whenNegatingSinglePattern_exceptionIsThrown () {
        try(Transaction tx = negationSession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
            Negation<Pattern> pattern = not(var("x").has("attribute", "value"));
            reasonerQueryFactory.composite(Iterables.getOnlyElement(pattern.getNegationDNF().getPatterns()));
        }
    }

    @Test (expected = ReasonerException.class)
    public void whenIncorrectlyBoundNestedNegationBlock_exceptionIsThrown () {
        try(Transaction tx = negationSession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
            Conjunction<?> pattern = and(
                    var("r").isa("entity"),
                    not(
                            and(
                                    var().rel("r2").rel("i"),
                                    and(var("i").isa("entity"))
                            )
                    )
            );
            reasonerQueryFactory.composite(Iterables.getOnlyElement(pattern.getNegationDNF().getPatterns()));
        }
    }

    @Test (expected = GraqlSemanticException.class)
    public void whenNegationBlockContainsDisjunction_exceptionIsThrown(){
        try(Transaction tx = negationSession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
            Conjunction<?> pattern = and(
                    var("x").isa("someType"),
                    not(
                            or(
                                    var("x").has("resource-string", "value"),
                                    var("x").has("resource-string", "someString")
                            )
                    )
            );
            reasonerQueryFactory.composite(Iterables.getOnlyElement(pattern.getNegationDNF().getPatterns()));
        }
    }

    @Test (expected = GraqlSemanticException.class)
    public void whenExecutingNegationQueryWithReasoningOff_exceptionIsThrown () {
        try(Transaction tx = negationSession.transaction(Transaction.Type.WRITE)) {
            Conjunction<?> pattern = and(
                    var("x").isa("entity"),
                    not(
                            var("x").has("attribute", "value")
                    )
            );
            tx.execute(match(pattern), false);
        }
    }

    @Test
    public void excludingAConceptWithSpecificId(){
        try (Transaction tx = negationSession.transaction(Transaction.Type.WRITE)) {
            Concept specificConcept = tx.stream(
                    match(var("x").isa("someType").has("resource-string", "value")).get()
            ).findFirst().orElse(null).get("x");

            GraqlGet query = match(
                    var().rel(var("rp")),
                    not(var("rp").id(specificConcept.id().getValue()))
                    ).get();

            List<ConceptMap> answers = tx.execute(query.asGet());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> assertNotEquals(ans.get("rp"), specificConcept));
        }
    }
}
