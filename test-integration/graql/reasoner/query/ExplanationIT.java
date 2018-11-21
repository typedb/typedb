//package grakn.core.graql.internal.reasoner;
//
//import grakn.core.server.Transaction;
//import Concept;
//import grakn.core.graql.query.GetQuery;
//import grakn.core.graql.query.QueryBuilder;
//import grakn.core.graql.query.Var;
//import grakn.core.graql.admin.Explanation;
//import grakn.core.graql.admin.ReasonerQuery;
//import grakn.core.graql.answer.ConceptMap;
//import grakn.core.graql.query.answer.ConceptMapImpl;
//import com.google.common.collect.ImmutableList;
//import com.google.common.collect.ImmutableMap;
//import com.google.common.collect.Iterables;
//import com.google.common.collect.Sets;
//import org.junit.BeforeClass;
//import org.junit.ClassRule;
//import org.junit.Test;
//
//import java.util.Collection;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//import java.util.stream.Collectors;
//
//import static grakn.core.graql.query.Graql.var;
//import static junit.framework.TestCase.assertTrue;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;
//
//public class ExplanationIT {
//
//    @ClassRule
//    public static final SampleKBContext geoKB = GeoKB.context();
//
//    @ClassRule
//    public static final SampleKBContext genealogyKB = GenealogyKB.context();
//
//    @ClassRule
//    public static final SampleKBContext explanationKB = SampleKBContext.load("explanationTest.gql");
//
//    @ClassRule
//    public static final SampleKBContext explanationKB2 = SampleKBContext.load("explanationTest2.gql");
//
//    @ClassRule
//    public static final SampleKBContext explanationKB3 = SampleKBContext.load("explanationTest3.gql");
//
//    private static Concept polibuda, uw;
//    private static Concept warsaw;
//    private static Concept masovia;
//    private static Concept poland;
//    private static Concept europe;
//    private static QueryBuilder iqb;
//
//    @BeforeClass
//    public static void onStartup() throws Exception {
//        Transaction tx = geoKB.tx();
//        iqb = tx.graql().infer(true);
//        polibuda = getConcept(tx, "name", "Warsaw-Polytechnics");
//        uw = getConcept(tx, "name", "University-of-Warsaw");
//        warsaw = getConcept(tx, "name", "Warsaw");
//        masovia = getConcept(tx, "name", "Masovia");
//        poland = getConcept(tx, "name", "Poland");
//        europe = getConcept(tx, "name", "Europe");
//    }
//
//    @Test
//    public void testExplanationTreeCorrect_TransitiveClosure() {
//        String queryString = "match (geo-entity: $x, entity-location: $y) isa is-located-in; get;";
//
//        ConceptMap answer1 = new ConceptMapImpl(ImmutableMap.of(var("x"), polibuda, var("y"), warsaw));
//        ConceptMap answer2 = new ConceptMapImpl(ImmutableMap.of(var("x"), polibuda, var("y"), masovia));
//        ConceptMap answer3 = new ConceptMapImpl(ImmutableMap.of(var("x"), polibuda, var("y"), poland));
//        ConceptMap answer4 = new ConceptMapImpl(ImmutableMap.of(var("x"), polibuda, var("y"), europe));
//
//        List<ConceptMap> answers = iqb.<GetQuery>parse(queryString).execute();
//        testExplanation(answers);
//
//        ConceptMap queryAnswer1 = findAnswer(answer1, answers);
//        ConceptMap queryAnswer2 = findAnswer(answer2, answers);
//        ConceptMap queryAnswer3 = findAnswer(answer3, answers);
//        ConceptMap queryAnswer4 = findAnswer(answer4, answers);
//
//        assertEquals(queryAnswer1, answer1);
//        assertEquals(queryAnswer2, answer2);
//        assertEquals(queryAnswer3, answer3);
//        assertEquals(queryAnswer4, answer4);
//
//        assertEquals(0, queryAnswer1.explanation().deductions().size());
//        assertEquals(2, queryAnswer2.explanation().deductions().size());
//        assertEquals(4, queryAnswer3.explanation().deductions().size());
//        assertEquals(6, queryAnswer4.explanation().deductions().size());
//
//        assertTrue(queryAnswer1.explanation().isLookupExplanation());
//
//        assertTrue(queryAnswer2.explanation().isRuleExplanation());
//        assertEquals(2, getLookupExplanations(queryAnswer2).size());
//        assertEquals(2, queryAnswer2.explanation().explicit().size());
//
//        assertTrue(queryAnswer3.explanation().isRuleExplanation());
//        assertEquals(2, getRuleExplanations(queryAnswer3).size());
//        assertEquals(3, queryAnswer3.explanation().explicit().size());
//
//        assertTrue(queryAnswer4.explanation().isRuleExplanation());
//        assertEquals(3, getRuleExplanations(queryAnswer4).size());
//        assertEquals(4, queryAnswer4.explanation().explicit().size());
//    }
//
//    @Test
//    public void testExplanationTreeCorrect_TransitiveClosureWithSpecificResourceAndTypes() {
//        String queryString = "match $x isa university;" +
//                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
//                "$y isa country;$y has name 'Poland'; get;";
//
//        ConceptMap answer1 = new ConceptMapImpl(ImmutableMap.of(var("x"), polibuda, var("y"), poland));
//        ConceptMap answer2 = new ConceptMapImpl(ImmutableMap.of(var("x"), uw, var("y"), poland));
//
//        List<ConceptMap> answers = iqb.<GetQuery>parse(queryString).execute();
//        testExplanation(answers);
//
//        ConceptMap queryAnswer1 = findAnswer(answer1, answers);
//        ConceptMap queryAnswer2 = findAnswer(answer2, answers);
//        assertEquals(queryAnswer1, answer1);
//        assertEquals(queryAnswer2, answer2);
//
//        assertTrue(queryAnswer1.explanation().isJoinExplanation());
//        assertTrue(queryAnswer2.explanation().isJoinExplanation());
//
//        //(res), (uni, ctr) - (region, ctr)
//        //                  - (uni, region) - {(city, region), (uni, city)
//        assertEquals(6, queryAnswer1.explanation().deductions().size());
//        assertEquals(6, queryAnswer2.explanation().deductions().size());
//
//        assertEquals(4, getLookupExplanations(queryAnswer1).size());
//        assertEquals(4, queryAnswer1.explanation().explicit().size());
//
//        assertEquals(4, getLookupExplanations(queryAnswer2).size());
//        assertEquals(4, queryAnswer2.explanation().explicit().size());
//    }
//
//    @Test
//    public void testExplanationTreeCorrect_QueryingSpecificAnswer(){
//        String queryString = "match " +
//                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
//                "$x id '" + polibuda.id() + "';" +
//                "$y id '" + europe.id() + "'; get;";
//
//        GetQuery query = iqb.parse(queryString);
//        List<ConceptMap> answers = query.execute();
//        assertEquals(answers.size(), 1);
//
//        ConceptMap answer = answers.iterator().next();
//        assertTrue(answer.explanation().isRuleExplanation());
//        assertEquals(2, answer.explanation().getAnswers().size());
//        assertEquals(3, getRuleExplanations(answer).size());
//        assertEquals(4, answer.explanation().explicit().size());
//        testExplanation(answers);
//    }
//
//    @Test
//    public void testExplainingConjunctiveQueryWithTwoIdPredicates(){
//        String queryString = "match " +
//                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
//                "(geo-entity: $y, entity-location: $z) isa is-located-in;" +
//                "$x id '" + polibuda.id() + "';" +
//                "$z id '" + masovia.id() + "';" +
//                "get $y;";
//
//        GetQuery query = iqb.parse(queryString);
//        List<ConceptMap> answers = query.execute();
//        assertEquals(answers.size(), 1);
//        testExplanation(answers);
//    }
//
//    @Test
//    public void testExplainingQueryContainingContradiction(){
//        String queryString = "match " +
//                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
//                "$x id '" + polibuda.id() + "';" +
//                "$y id '" + uw.id() + "'; get;";
//
//        GetQuery query = iqb.parse(queryString);
//        List<ConceptMap> answers = query.execute();
//        assertEquals(answers.size(), 0);
//    }
//
//    @Test
//    public void testExplainingNonRuleResolvableQuery(){
//        String queryString = "match $x isa city, has name $n; get;";
//
//        GetQuery query = iqb.parse(queryString);
//        List<ConceptMap> answers = query.execute();
//        answers.forEach(ans -> assertEquals(ans.explanation().isEmpty(), true));
//    }
//
//    @Test
//    public void testExplainingQueryContainingContradiction2(){
//        Transaction expGraph = explanationKB.tx();
//        QueryBuilder eiqb = expGraph.graql().infer(true);
//
//        Concept a1 = getConcept(expGraph, "name", "a1");
//        Concept a2 = getConcept(expGraph, "name", "a2");
//        String queryString = "match " +
//                "(role1: $x, role2: $y) isa baseRelation;" +
//                "$x id '" + a1.id() + "';" +
//                "$y id '" + a2.id() + "'; get;";
//
//        GetQuery query = eiqb.parse(queryString);
//        List<ConceptMap> answers = query.execute();
//        assertEquals(answers.size(), 0);
//    }
//
//    @Test
//    public void testExplainingConjunctions(){
//        Transaction expGraph = explanationKB.tx();
//        QueryBuilder eiqb = expGraph.graql().infer(true);
//
//        String queryString = "match " +
//                "(role1: $x, role2: $w) isa inferredRelation;" +
//                "$x has name $xName;" +
//                "$w has name $wName; get;";
//
//        GetQuery query = eiqb.parse(queryString);
//        List<ConceptMap> answers = query.execute();
//        testExplanation(answers);
//    }
//
//    @Test
//    public void testExplainingMixedAtomicQueries(){
//        Transaction expGraph = explanationKB2.tx();
//        QueryBuilder eiqb = expGraph.graql().infer(true);
//
//        String queryString = "match " +
//                "$x has value 'high';" +
//                "($x, $y) isa carried-relation;" +
//                "get;";
//
//        GetQuery query = eiqb.parse(queryString);
//        List<ConceptMap> answers = query.execute();
//        testExplanation(answers);
//        answers.stream()
//                .filter(ans -> ans.explanations().stream().anyMatch(Explanation::isRuleExplanation))
//                .forEach( inferredAnswer -> {
//                    Set<Explanation> explanations = inferredAnswer.explanations();
//                    assertEquals(explanations.stream().filter(Explanation::isRuleExplanation).count(), 2);
//                    assertEquals(explanations.stream().filter(Explanation::isLookupExplanation).count(), 4);
//                });
//    }
//
//    @Test
//    public void testExplainingEquivalentPartialQueries(){
//        Transaction expGraph = explanationKB3.tx();
//        QueryBuilder eiqb = expGraph.graql().infer(true);
//
//        String queryString = "match $x isa same-tag-column-link; get;";
//
//        GetQuery query = eiqb.parse(queryString);
//        List<ConceptMap> answers = query.execute();
//        testExplanation(answers);
//        answers.stream()
//                .filter(ans -> ans.explanations().stream().anyMatch(Explanation::isRuleExplanation))
//                .forEach( inferredAnswer -> {
//                    Set<Explanation> explanations = inferredAnswer.explanations();
//                    assertEquals(explanations.stream().filter(Explanation::isRuleExplanation).count(), 1);
//                    assertEquals(explanations.stream().filter(Explanation::isLookupExplanation).count(), 3);
//                });
//    }
//
//    @Test
//    public void testExplanationConsistency(){
//        Transaction genealogyGraph = genealogyKB.tx();
//        final long limit = 3;
//        QueryBuilder iqb = genealogyGraph.graql().infer(true);
//        String queryString = "match " +
//                "($x, $y) isa cousins;" +
//                "limit " + limit + ";"+
//                "get;";
//
//        List<ConceptMap> answers = iqb.<GetQuery>parse(queryString).execute();
//
//        assertEquals(answers.size(), limit);
//        answers.forEach(answer -> {
//            testExplanation(answer);
//
//            String specificQuery = "match " +
//                    "$x id '" + answer.get(var("x")).id().getValue() + "';" +
//                    "$y id '" + answer.get(var("y")).id().getValue() + "';" +
//                    "(cousin: $x, cousin: $y) isa cousins;" +
//                    "limit 1; get;";
//            ConceptMap specificAnswer = Iterables.getOnlyElement(iqb.<GetQuery>parse(specificQuery).execute());
//            assertEquals(answer, specificAnswer);
//            testExplanation(specificAnswer);
//        });
//    }
//
//    private void testExplanation(Collection<ConceptMap> answers){
//        answers.forEach(this::testExplanation);
//    }
//
//    private void testExplanation(ConceptMap answer){
//        answerHasConsistentExplanations(answer);
//        checkExplanationCompleteness(answer);
//        checkAnswerConnectedness(answer);
//    }
//
//    //ensures that each branch ends up with an lookup explanation
//    private void checkExplanationCompleteness(ConceptMap answer){
//        assertFalse("Non-lookup explanation misses children",
//                answer.explanations().stream()
//                        .filter(e -> !e.isLookupExplanation())
//                        .anyMatch(e -> e.getAnswers().isEmpty())
//        );
//    }
//
//    private void checkAnswerConnectedness(ConceptMap answer){
//        ImmutableList<ConceptMap> answers = answer.explanation().getAnswers();
//        answers.forEach(a -> {
//            assertTrue("Disconnected answer in explanation",
//                    answers.stream()
//                            .filter(a2 -> !a2.equals(a))
//                            .anyMatch(a2 -> !Sets.intersection(a.vars(), a2.vars()).isEmpty())
//            );
//        });
//    }
//
//    private void answerHasConsistentExplanations(ConceptMap answer){
//        Set<ConceptMap> answers = answer.explanation().deductions().stream()
//                .filter(a -> !a.explanation().isJoinExplanation())
//                .collect(Collectors.toSet());
//
//        answers.forEach(a -> assertTrue("Answer has inconsistent explanations", explanationConsistentWithAnswer(a)));
//    }
//
//    private static Concept getConcept(Transaction graph, String typeLabel, Object val){
//        return graph.graql().match(var("x").has(typeLabel, val)).get("x")
//                .stream().map(ans -> ans.get("x")).findAny().orElse(null);
//    }
//
//    private ConceptMap findAnswer(ConceptMap a, List<ConceptMap> list){
//        for(ConceptMap ans : list) {
//            if (ans.equals(a)) return ans;
//        }
//        return new ConceptMapImpl();
//    }
//
//    private Set<Explanation> getRuleExplanations(ConceptMap a){
//        return a.explanations().stream().filter(Explanation::isRuleExplanation).collect(Collectors.toSet());
//    }
//
//    private Set<Explanation> getLookupExplanations(ConceptMap a){
//        return a.explanations().stream().filter(Explanation::isLookupExplanation).collect(Collectors.toSet());
//    }
//
//    private boolean explanationConsistentWithAnswer(ConceptMap ans){
//        ReasonerQuery query = ans.explanation().getQuery();
//        Set<Var> vars = query != null? query.getVarNames() : new HashSet<>();
//        return vars.containsAll(ans.map().keySet());
//    }
//}