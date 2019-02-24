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

package grakn.core.graql.reasoner.cache;

import com.google.common.collect.Sets;
import grakn.core.concept.Label;
import grakn.core.concept.Rule;
import grakn.core.concept.Type;
import grakn.core.graql.internal.reasoner.rule.InferenceRule;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import grakn.core.server.session.cache.RuleCache;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.stream.Collectors;

import static graql.lang.Graql.type;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class RuleCacheIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl ruleApplicabilitySession;

    private static void loadFromFile(String fileName, Session session) {
        try {
            InputStream inputStream = RuleCacheIT.class.getClassLoader().getResourceAsStream("test-integration/graql/reasoner/resources/" + fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            Graql.parseList(s).forEach(tx::execute);
            tx.commit();
        } catch (Exception e) {
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void loadContext() {
        ruleApplicabilitySession = server.sessionWithNewKeyspace();
        loadFromFile("ruleApplicabilityTest.gql", ruleApplicabilitySession);

    }

    @AfterClass
    public static void closeSession() {
        ruleApplicabilitySession.close();
    }

    @Test
    public void whenGettingRulesWithType_correctRulesAreObtained(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            RuleCache ruleCache = tx.ruleCache();

            Type reifyingRelation = tx.getType(Label.of("reifying-relation"));
            Type ternary = tx.getType(Label.of("ternary"));
            Set<Rule> rulesWithBinary = ruleCache.getRulesWithType(reifyingRelation).collect(toSet());
            Set<Rule> rulesWithTernary = ruleCache.getRulesWithType(ternary).collect(toSet());

            assertEquals(2, rulesWithBinary.size());
            assertEquals(2, rulesWithTernary.size());

            rulesWithBinary.stream()
                    .map(r -> ruleCache.getRule(r, () -> new InferenceRule(r, tx)))
                    .forEach(r -> assertEquals(reifyingRelation, r.getHead().getAtom().getSchemaConcept()));
            rulesWithTernary.stream()
                    .map(r -> ruleCache.getRule(r, () -> new InferenceRule(r, tx)))
                    .forEach(r -> assertEquals(ternary, r.getHead().getAtom().getSchemaConcept()));
        }
    }

    @Test
    public void whenAddingARule_cacheContainsUpdatedEntry(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            Pattern when = Graql.parsePattern("{ $x isa entity;$y isa entity; };");
            Pattern then = Graql.parsePattern("{ (someRole: $x, subRole: $y) isa binary; };");
            Rule dummyRule = tx.putRule("dummyRule", when, then);

            Type binary = tx.getType(Label.of("binary"));
            Set<Rule> cachedRules = tx.ruleCache().getRulesWithType(binary).collect(Collectors.toSet());
            assertTrue(cachedRules.contains(dummyRule));
        }
    }

    @Test
    public void whenAddingARuleAfterClosingTx_cacheContainsConsistentEntry(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {

            Pattern when = Graql.parsePattern("{ $x isa entity;$y isa entity; };");
            Pattern then = Graql.parsePattern("{ (someRole: $x, subRole: $y) isa binary; };");
            Rule dummyRule = tx.putRule("dummyRule", when, then);

            Type binary = tx.getType(Label.of("binary"));
            Set<Rule> commitedRules = binary.thenRules().collect(Collectors.toSet());
            Set<Rule> cachedRules = tx.ruleCache().getRulesWithType(binary).collect(toSet());
            assertEquals(Sets.union(commitedRules, Sets.newHashSet(dummyRule)), cachedRules);
        }
    }

    @Test
    public void whenDeletingARule_cacheContainsUpdatedEntry(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.undefine(type("rule-0").sub("rule")));
            tx.commit();
        }
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            Type binary = tx.getType(Label.of("binary"));
            Set<Rule> rules = tx.ruleCache().getRulesWithType(binary).collect(Collectors.toSet());
            assertTrue(rules.isEmpty());
        }
    }


    private Conjunction<Statement> conjunction(String patternString){
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}
