package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Label;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.answer.ConceptMapImpl;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.kb.internal.cache.TxRuleCache;
import ai.grakn.test.rule.ConcurrentGraknServer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
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

import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class RuleCacheIT {

    @ClassRule
    public static final ConcurrentGraknServer server = new ConcurrentGraknServer();

    private static EmbeddedGraknSession ruleApplicabilitySession;

    private static void loadFromFile(String fileName, GraknSession session) {
        try {
            InputStream inputStream = RuleCacheIT.class.getClassLoader().getResourceAsStream("test-integration/test/graql/reasoner/resources/" + fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            GraknTx tx = session.transaction(GraknTxType.WRITE);
            tx.graql().parser().parseList(s).forEach(Query::execute);
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
    private EmbeddedGraknTx tx;


    @Before
    public void onStartup(){
        tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);
        String recordPatternString = "{(someRole: $x, subRole: $y) isa reifiable-relation;}";
        String retrievePatternString = "{(someRole: $p1, subRole: $p2) isa reifiable-relation;}";
        Conjunction<VarPatternAdmin> recordPattern = conjunction(recordPatternString, tx);
        Conjunction<VarPatternAdmin> retrievePattern = conjunction(retrievePatternString, tx);
        recordQuery = ReasonerQueries.atomic(recordPattern, tx);
        retrieveQuery = ReasonerQueries.atomic(retrievePattern, tx);
        retrieveToRecordUnifier = retrieveQuery.getMultiUnifier(recordQuery).getUnifier();
        recordToRetrieveUnifier = retrieveToRecordUnifier.inverse();

        Entity entity = tx.getEntityType("anotherNoRoleEntity").instances().findFirst().orElse(null);
        singleAnswer = new ConceptMapImpl(
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
        TxRuleCache ruleCache = tx.ruleCache();

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
        Pattern when = tx.graql().parser().parsePattern("{$x isa entity;$y isa entity;}");
        Pattern then = tx.graql().parser().parsePattern("{(someRole: $x, subRole: $y) isa binary;}");
        Rule dummyRule = tx.putRule("dummyRule", when, then);

        SchemaConcept binary = tx.getSchemaConcept(Label.of("binary"));
        Set<Rule> cachedRules = tx.ruleCache().getRulesWithType(binary).collect(Collectors.toSet());
        assertTrue(cachedRules.contains(dummyRule));
    }

    @Test
    public void whenAddingARuleAfterClosingTx_cacheContainsConsistentEntry(){
        tx.close();
        tx = ruleApplicabilitySession.transaction(GraknTxType.WRITE);

        Pattern when = tx.graql().parser().parsePattern("{$x isa entity;$y isa entity;}");
        Pattern then = tx.graql().parser().parsePattern("{(someRole: $x, subRole: $y) isa binary;}");
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
        tx.graql().parse("undefine $x sub rule label 'rule-0';").execute();

        SchemaConcept binary = tx.getSchemaConcept(Label.of("binary"));
        Set<Rule> rules = tx.ruleCache().getRulesWithType(binary).collect(Collectors.toSet());
        assertTrue(rules.isEmpty());
    }


    private Conjunction<VarPatternAdmin> conjunction(String patternString, GraknTx graph){
        Set<VarPatternAdmin> vars = graph.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}
