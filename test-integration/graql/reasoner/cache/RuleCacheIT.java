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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.Rule;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.rule.InferenceRule;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.UndefineQuery;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import grakn.core.server.session.cache.RuleCache;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.graql.query.pattern.Pattern.var;
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

    private static ReasonerAtomicQuery recordQuery;
    private static ReasonerAtomicQuery retrieveQuery;
    private static ConceptMap singleAnswer;
    private static Unifier retrieveToRecordUnifier;
    private static Unifier recordToRetrieveUnifier;
    private TransactionOLTP tx;


    @Before
    public void onStartup(){
        tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE);
        String recordPatternString = "{(someRole: $x, subRole: $y) isa reifiable-relation;}";
        String retrievePatternString = "{(someRole: $p1, subRole: $p2) isa reifiable-relation;}";
        Conjunction<Statement> recordPattern = conjunction(recordPatternString);
        Conjunction<Statement> retrievePattern = conjunction(retrievePatternString);
        recordQuery = ReasonerQueries.atomic(recordPattern, tx);
        retrieveQuery = ReasonerQueries.atomic(retrievePattern, tx);
        retrieveToRecordUnifier = retrieveQuery.getMultiUnifier(recordQuery).getUnifier();
        recordToRetrieveUnifier = retrieveToRecordUnifier.inverse();

        Entity entity = tx.getEntityType("anotherNoRoleEntity").instances().findFirst().orElse(null);
        singleAnswer = new ConceptMap(
                ImmutableMap.of(
                        var("x"), entity,
                        var("y"), entity
                ));
    }

    @After
    public void closeTx() {
        tx.close();
    }


    @Test
    public void whenGettingRulesWithType_correctRulesAreObtained(){
        RuleCache ruleCache = tx.ruleCache();

        SchemaConcept reifyingRelation = tx.getSchemaConcept(Label.of("reifying-relation"));
        SchemaConcept ternary = tx.getSchemaConcept(Label.of("ternary"));
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

    @Test
    public void whenAddingARule_cacheContainsUpdatedEntry(){
        Pattern when = Pattern.parse("{$x isa entity;$y isa entity;}");
        Pattern then = Pattern.parse("{(someRole: $x, subRole: $y) isa binary;}");
        Rule dummyRule = tx.putRule("dummyRule", when, then);

        SchemaConcept binary = tx.getSchemaConcept(Label.of("binary"));
        Set<Rule> cachedRules = tx.ruleCache().getRulesWithType(binary).collect(Collectors.toSet());
        assertTrue(cachedRules.contains(dummyRule));
    }

    @Test
    public void whenAddingARuleAfterClosingTx_cacheContainsConsistentEntry(){
        tx.close();
        tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE);

        Pattern when = Pattern.parse("{$x isa entity;$y isa entity;}");
        Pattern then = Pattern.parse("{(someRole: $x, subRole: $y) isa binary;}");
        Rule dummyRule = tx.putRule("dummyRule", when, then);

        SchemaConcept binary = tx.getSchemaConcept(Label.of("binary"));

        Set<Rule> commitedRules = binary.thenRules().collect(Collectors.toSet());
        Set<Rule> cachedRules = tx.ruleCache().getRulesWithType(binary).collect(toSet());
        assertEquals(Sets.union(commitedRules, Sets.newHashSet(dummyRule)), cachedRules);
    }

    //TODO: currently we do not acknowledge deletions
    @Ignore
    @Test
    public void whenDeletingARule_cacheContainsUpdatedEntry(){
        tx.execute(Graql.<UndefineQuery>parse("undefine $x sub rule label 'rule-0';"));

        SchemaConcept binary = tx.getSchemaConcept(Label.of("binary"));
        Set<Rule> rules = tx.ruleCache().getRulesWithType(binary).collect(Collectors.toSet());
        assertTrue(rules.isEmpty());
    }


    private Conjunction<Statement> conjunction(String patternString){
        Set<Statement> vars = Pattern.parse(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Pattern.and(vars);
    }
}
