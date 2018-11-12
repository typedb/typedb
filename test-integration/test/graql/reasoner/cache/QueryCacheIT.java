package grakn.core.graql.internal.reasoner;

import grakn.core.GraknSession;
import grakn.core.GraknTx;
import grakn.core.GraknTxType;
import grakn.core.concept.Entity;
import grakn.core.factory.EmbeddedGraknSession;
import grakn.core.graql.Query;
import grakn.core.graql.admin.Conjunction;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.pattern.Patterns;
import grakn.core.graql.internal.query.answer.ConceptMapImpl;
import grakn.core.graql.internal.reasoner.cache.SimpleQueryCache;
import grakn.core.graql.internal.reasoner.query.QueryAnswers;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.kb.internal.EmbeddedGraknTx;
import grakn.core.test.rule.ConcurrentGraknServer;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.graql.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class QueryCacheIT {

    @ClassRule
    public static final ConcurrentGraknServer server = new ConcurrentGraknServer();

    private static EmbeddedGraknSession ruleApplicabilitySession;

    private static void loadFromFile(String fileName, GraknSession session) {
        try {
            InputStream inputStream = QueryCacheIT.class.getClassLoader().getResourceAsStream("test-integration/test/graql/reasoner/resources/" + fileName);
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
    public void recordRetrieveAnswers(){
        SimpleQueryCache<ReasonerAtomicQuery> cache = new SimpleQueryCache<>();
        QueryAnswers record = cache.record(recordQuery, new QueryAnswers(recordQuery.getQuery().execute()));
        assertEquals(record, cache.getAnswers(retrieveQuery).unify(retrieveToRecordUnifier));
        assertEquals(record, cache.getAnswers(recordQuery));
    }

    @Test
    public void recordUpdateRetrieveAnswers(){
        SimpleQueryCache<ReasonerAtomicQuery> cache = new SimpleQueryCache<>();
        cache.record(recordQuery, new QueryAnswers(recordQuery.getQuery().execute()));
        cache.record(recordQuery, singleAnswer);
        assertTrue(cache.getAnswers(recordQuery).contains(singleAnswer));
        assertTrue(cache.getAnswers(retrieveQuery).contains(singleAnswer.unify(recordToRetrieveUnifier)));
    }

    @Test
    public void recordRetrieveAnswerStream(){
        SimpleQueryCache<ReasonerAtomicQuery> cache = new SimpleQueryCache<>();
        Set<ConceptMap> record = cache.record(recordQuery, recordQuery.getQuery().stream()).collect(Collectors.toSet());
        assertEquals(record, cache.getAnswerStream(retrieveQuery).map(ans -> ans.unify(retrieveToRecordUnifier)).collect(Collectors.toSet()));
        assertEquals(record, cache.record(recordQuery, recordQuery.getQuery().stream()).collect(Collectors.toSet()));
    }

    @Test
    public void recordUpdateRetrieveAnswerStream(){
        SimpleQueryCache<ReasonerAtomicQuery> cache = new SimpleQueryCache<>();
        cache.record(recordQuery, recordQuery.getQuery().stream());
        cache.record(recordQuery, singleAnswer);

        assertTrue(cache.getAnswerStream(recordQuery).anyMatch(ans -> ans.equals(singleAnswer)));
        assertTrue(cache.getAnswerStream(retrieveQuery).anyMatch(ans -> ans.equals(singleAnswer.unify(recordToRetrieveUnifier))));
    }

    @Test
    public void getRetrieveAnswerStream() {
        SimpleQueryCache<ReasonerAtomicQuery> cache = new SimpleQueryCache<>();
        ConceptMap answer = recordQuery.getQuery().stream().findFirst().orElse(null);
        ConceptMap retrieveAnswer = answer.unify(recordToRetrieveUnifier);

        Stream<ConceptMap> recordStream = cache.getAnswerStream(recordQuery);
        Stream<ConceptMap> retrieveStream = cache.getAnswerStream(retrieveQuery);

        QueryAnswers records = new QueryAnswers(recordStream.collect(Collectors.toSet()));
        QueryAnswers retrieveAnswers = new QueryAnswers(retrieveStream.collect(Collectors.toSet()));

        assertTrue(records.contains(answer));
        assertTrue(retrieveAnswers.contains(retrieveAnswer));
    }

    @Test
    public void getUpdateRetrieveAnswerStream() {
        SimpleQueryCache<ReasonerAtomicQuery> cache = new SimpleQueryCache<>();
        ConceptMap answer = recordQuery.getQuery().stream().findFirst().orElse(null);
        ConceptMap retrieveAnswer = answer.unify(recordToRetrieveUnifier);
        ConceptMap retrieveSingleAnswer = singleAnswer.unify(recordToRetrieveUnifier);
        Stream<ConceptMap> recordStream = cache.getAnswerStream(recordQuery);
        Stream<ConceptMap> retrieveStream = cache.getAnswerStream(retrieveQuery);

        cache.record(recordQuery, singleAnswer);

        QueryAnswers records = new QueryAnswers(recordStream.collect(Collectors.toSet()));
        QueryAnswers retrieveAnswers = new QueryAnswers(retrieveStream.collect(Collectors.toSet()));

        //NB: not expecting the update in the stream
        assertTrue(records.contains(answer));
        assertTrue(retrieveAnswers.contains(retrieveAnswer));
        assertFalse(records.contains(singleAnswer));
        assertFalse(retrieveAnswers.contains(retrieveSingleAnswer));

        assertTrue(cache.getAnswers(recordQuery).contains(singleAnswer));
        assertTrue(cache.getAnswers(retrieveQuery).contains(retrieveSingleAnswer));
    }

    @Test
    public void recordRetrieveSingleAnswer(){
        SimpleQueryCache<ReasonerAtomicQuery> cache = new SimpleQueryCache<>();
        ConceptMap answer = recordQuery.getQuery().stream().findFirst().orElse(null);
        ConceptMap retrieveAnswer = answer.unify(recordToRetrieveUnifier);
        cache.record(recordQuery, answer);

        assertEquals(cache.getAnswer(recordQuery, new ConceptMapImpl()), new ConceptMapImpl());
        assertEquals(cache.getAnswer(recordQuery, answer), answer);
        assertEquals(cache.getAnswer(recordQuery, retrieveAnswer), answer);

        assertEquals(cache.getAnswer(retrieveQuery, new ConceptMapImpl()), new ConceptMapImpl());
        assertEquals(cache.getAnswer(retrieveQuery, retrieveAnswer), retrieveAnswer);
        assertEquals(cache.getAnswer(retrieveQuery, answer), retrieveAnswer);
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, GraknTx tx) {
        Set<VarPatternAdmin> vars = tx.graql().parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}
