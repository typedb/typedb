package ai.grakn.test.graql.reasoner;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graphs.GeoGraph;
import ai.grakn.graphs.MatrixGraphII;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.cache.LazyQueryCache;
import ai.grakn.graql.internal.reasoner.query.QueryAnswerStream;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.test.GraphContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.varFilterFunction;
import static ai.grakn.test.GraknTestEnv.usingTinker;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/**
 * Created by kasper on 10/02/17.
 */
public class LazyTest {

    @Rule
    public final GraphContext geoGraph = GraphContext.preLoad(GeoGraph.get());

    @ClassRule
    public static final GraphContext graphContext = GraphContext.empty();

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(usingTinker());
    }

    @Test
    public void testLazyCache(){
        GraknGraph graph = geoGraph.graph();
        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        String patternString2 = "{(geo-entity: $y, entity-location: $z) isa is-located-in;}";
        LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarAdmin> pattern2 = conjunction(patternString2, graph);
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(pattern, graph);
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(pattern2, graph);

        Stream<Map<VarName, Concept>> dbStream = query.DBlookup();
        cache.record(query, dbStream);

        Set<Map<VarName, Concept>> collect = cache.getAnswerStream(query).collect(toSet());
        Set<Map<VarName, Concept>> collect2 = cache.getAnswerStream(query2).collect(toSet());
        assertEquals(collect.size(), collect2.size());
    }

    @Test
    public void testLazyCache2(){
        GraknGraph graph = geoGraph.graph();
        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        String patternString2 = "{(geo-entity: $y, entity-location: $z) isa is-located-in;}";
        String patternString3 = "{(geo-entity: $x, entity-location: $z) isa is-located-in;}";
        LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarAdmin> pattern3 = conjunction(patternString3, graph);
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(pattern, graph);
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(pattern2, graph);
        ReasonerAtomicQuery query3 = new ReasonerAtomicQuery(pattern3, graph);

        Stream<Map<VarName, Concept>> stream = query.lookup(cache);
        Stream<Map<VarName, Concept>> stream2 = query2.lookup(cache);
        Stream<Map<VarName, Concept>> joinedStream = QueryAnswerStream.join(stream, stream2);

        joinedStream = cache.record(query3, joinedStream.flatMap(a -> varFilterFunction.apply(a, query3.getVarNames())));

        Set<Map<VarName, Concept>> collect = joinedStream.collect(toSet());
        Set<Map<VarName, Concept>> collect2 = cache.getAnswerStream(query3).collect(toSet());

        assertEquals(collect.size(), 37);
        assertEquals(collect.size(), collect2.size());
    }

    @Test
    public void testJoin(){
        GraknGraph graph = geoGraph.graph();
        String patternString = "{(geo-entity: $x, entity-location: $y) isa is-located-in;}";
        String patternString2 = "{(geo-entity: $y, entity-location: $z) isa is-located-in;}";
        String patternString3 = "{(geo-entity: $z, entity-location: $w) isa is-located-in;}";
        LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
        Conjunction<VarAdmin> pattern = conjunction(patternString, graph);
        Conjunction<VarAdmin> pattern2 = conjunction(patternString2, graph);
        Conjunction<VarAdmin> pattern3 = conjunction(patternString3, graph);
        ReasonerAtomicQuery query = new ReasonerAtomicQuery(pattern, graph);
        ReasonerAtomicQuery query2 = new ReasonerAtomicQuery(pattern2, graph);
        ReasonerAtomicQuery query3 = new ReasonerAtomicQuery(pattern3, graph);

        Stream<Map<VarName, Concept>> stream = query.lookup(cache);
        Stream<Map<VarName, Concept>> stream2 = query2.lookup(cache);
        Stream<Map<VarName, Concept>> stream3 = query3.lookup(cache);

        Stream<Map<VarName, Concept>> join = QueryAnswerStream.join(QueryAnswerStream.join(stream, stream2), stream3);
        assertEquals(join.collect(toSet()).size(), 10);
    }

    @Test
    public void testKnownFilter(){
        GraknGraph graph = geoGraph.graph();
        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in;";
        MatchQuery query = graph.graql().parse(queryString);
        QueryAnswers answers = new QueryAnswers(query.admin().streamWithVarNames().collect(toSet()));
        long count = query.admin()
                .streamWithVarNames()
                .filter(a -> QueryAnswerStream.knownFilter(a, answers.stream()))
                .count();
        assertEquals(count, 0);
    }

    @Test //(timeout = 30000)
    public void testLazy()  {
        final int N = 30;

        long startTime = System.currentTimeMillis();
        MatrixGraphII.getGraph(N, N).accept(graphContext.graph());
        long loadTime = System.currentTimeMillis() - startTime;
        System.out.println("loadTime: " + loadTime);
        GraknGraph graph = graphContext.graph();

        QueryBuilder iqb = graph.graql().infer(true).materialise(false);
        String queryString = "match (P-from: $x, P-to: $y) isa P;";
        MatchQuery query = iqb.parse(queryString);

        int limit = 100;
        startTime = System.currentTimeMillis();
        List<Map<String, Concept>> results = query.limit(limit).execute();
        long answerTime = System.currentTimeMillis() - startTime;
        System.out.println("limit " + limit + " results = " + results.size() + " answerTime: " + answerTime);
    }

    private Conjunction<VarAdmin> conjunction(String patternString, GraknGraph graph){
        Set<VarAdmin> vars = graph.graql().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}
