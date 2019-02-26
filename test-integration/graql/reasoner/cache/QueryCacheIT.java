package grakn.core.graql.reasoner.cache;
/*
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import grakn.core.server.Transaction;
import grakn.core.concept.instance.Entity;
import grakn.core.server.session.SessionImpl;
import grakn.core.graql.query.Query;
import graql.lang.pattern.Conjunction;
import Unifier;
import grakn.core.graql.query.pattern.VarPatternAdmin;
import Patterns;
import grakn.core.concept.answer.ConceptMap;
import SimpleQueryCache;
import QueryAnswers;
import ReasonerAtomicQuery;
import ReasonerQueries;
import grakn.core.server.session.TransactionImpl;
import grakn.core.rule.GraknTestServer;
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

import static graql.lang.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")

public class QueryCacheIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl ruleApplicabilitySession;

    private static void loadFromFile(String fileName, SessionImpl session) {
        try {
            InputStream inputStream = QueryCacheIT.class.getClassLoader().getResourceAsStream("test-integration/graql/reasoner/resources/" + fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            TransactionOLTP tx = session.transaction(Transaction.Type.WRITE);
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
    private TransactionImpl tx;


    @Before
    public void onStartup(){
        tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE);
        String recordPatternString = "{(someRole: $x, subRole: $y) isa reifiable-relation;}";
        String retrievePatternString = "{(someRole: $p1, subRole: $p2) isa reifiable-relation;}";
        Conjunction<VarPatternAdmin> recordPattern = conjunction(recordPatternString, tx);
        Conjunction<VarPatternAdmin> retrievePattern = conjunction(retrievePatternString, tx);
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

        assertEquals(cache.getAnswer(recordQuery, new ConceptMap()), new ConceptMap());
        assertEquals(cache.getAnswer(recordQuery, answer), answer);
        assertEquals(cache.getAnswer(recordQuery, retrieveAnswer), answer);

        assertEquals(cache.getAnswer(retrieveQuery, new ConceptMap()), new ConceptMap());
        assertEquals(cache.getAnswer(retrieveQuery, retrieveAnswer), retrieveAnswer);
        assertEquals(cache.getAnswer(retrieveQuery, answer), retrieveAnswer);
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString, TransactionOLTP tx) {
        Set<VarPatternAdmin> vars = Graql.parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}
*/