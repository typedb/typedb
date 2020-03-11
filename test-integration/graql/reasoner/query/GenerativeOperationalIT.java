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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import grakn.common.util.Pair;
import grakn.core.common.config.Config;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.graql.reasoner.utils.TarjanSCC;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import grakn.verification.tools.operator.Operator;
import grakn.verification.tools.operator.Operators;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static graql.lang.Graql.and;
import static graql.lang.Graql.var;

/**
 * Using our subsumption Operators, starting with a specific pattern, we generate large number of query pattern pairs
 * by continuous application of generalisation operations defined by the Operators.
 * As a result, we produce query pairs such that each pair is in a subsumption relationship (one pattern generifies the other).
 * Knowing that this property holds between the pairs, we can then test our query subsumption and query unification algorithms
 * as their output is known based on the subsumption relationship.
 *
 * Consequently for each `(parent, child)` query pattern pair we test whether:
 * - there exists a `RULE` unifier between the parent and the child
 * - there exists a `SUBSUMPTIVE` unifier between the parent and the child (child subsumes parent)
 * - there exists a `STRUCTURAL_SUBSUMPTIVE` unifier between the parent and the child
 */
public class GenerativeOperationalIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session genericSchemaSession;
    private static HashMultimap<Pattern, Pattern> binaryRelationPatternTree;
    private static HashMultimap<Pattern, Pattern> ternaryRelationPatternTree;

    @BeforeClass
    public static void loadContext() {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        genericSchemaSession = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        String resourcePath = "test-integration/graql/reasoner/resources/";
        loadFromFileAndCommit(resourcePath, "genericSchema.gql", genericSchemaSession);

        try(Transaction tx = genericSchemaSession.readTransaction()) {
            String id = tx.getEntityType("baseRoleEntity").instances().iterator().next().id().getValue();
            String subId = tx.getEntityType("subRoleEntity").instances().iterator().next().id().getValue();
            String subSubId = tx.getEntityType("subSubRoleEntity").instances().iterator().next().id().getValue();
            Pattern baseBinaryPattern = and(
                    var("r")
                            .rel("subRole1", var("x"))
                            .rel("subRole2", var("y"))
                            .isa("binary"),
                    var("x").isa("subRoleEntity"),
                    var("x").id(id),
                    var("y").isa("subRoleEntity"),
                    var("y").id(subId)//,
            );

            Pattern baseTernaryPattern = and(
                    var("r")
                            .rel("subRole1", var("x"))
                            .rel("subRole2", var("y"))
                            //NB: we duplicate the role as if we pick the third role type inference will always infer the type to be ternary
                            .rel("subSubRole2", var("z"))
                            .isa("ternary"),
                    var("x").isa("subRoleEntity"),
                    var("x").id(id),
                    var("y").isa("subRoleEntity"),
                    var("y").id(subId),
                    var("z").isa("subSubRoleEntity"),
                    var("z").id(subSubId)
            );
            binaryRelationPatternTree = generatePatternTree(
                    baseBinaryPattern,
                    new TransactionContext(tx),
                    Lists.newArrayList(Operators.typeGeneralise(), Operators.roleGeneralise()),
                    Integer.MAX_VALUE)
            ;

            ternaryRelationPatternTree = generatePatternTree(
                    baseTernaryPattern,
                    new TransactionContext(tx),
                    Lists.newArrayList(Operators.typeGeneralise(), Operators.roleGeneralise()),
                    Integer.MAX_VALUE)
            ;
        }
    }

    @AfterClass
    public static void closeSession() {
        genericSchemaSession.close();
    }

    /**
     * Generates a tree of patterns where such that each child is a generalisation of its parent pattern.
     * @param basePattern starting specific pattern
     * @param ctx schema(type) context
     * @return map containing parent->{children} mappings
     */
    private static HashMultimap<Pattern, Pattern> generatePatternTree(Pattern basePattern, TransactionContext ctx, List<Operator> ops, int maxOps){
        HashMultimap<Pattern, Pattern> patternTree = HashMultimap.create();
        Set<Pattern> output = Operators.removeSubstitution().apply(basePattern, ctx).collect(Collectors.toSet());

        int applications = 0;
        while (!(output.isEmpty() || applications > maxOps)){
            Stream<Pattern> pstream = output.stream();
            for(Operator op : ops){
                pstream = pstream.flatMap(parent -> op.apply(parent, ctx).peek(child -> patternTree.put(parent, child)));
            }
            output = pstream.collect(Collectors.toSet());
            applications++;
        }
        return patternTree;
    }

    /**
     * Generates pattern pairs to test from the pregenerated pattern tree.
     * @param exhaustive whether to compute full transitive closure of the parent-child relation.
     * @return stream of test case pattern pairs
     */
    private static Stream<Pair<Pattern, Pattern>> generateTestPairs(HashMultimap<Pattern, Pattern> tree, boolean exhaustive){
        //non-exhaustive option returns only the direct parent-child pairs
        //instead of full transitive closure of the parent-child relation
        if (!exhaustive){
            return tree.entries().stream().map(e -> new Pair<>(e.getKey(), e.getValue()));
        }

        TarjanSCC<Pattern> tarjan = new TarjanSCC<>(tree);
        return tarjan.successorMap().entries().stream().map(e -> new Pair<>(e.getKey(), e.getValue()));
    }

    @Test
    public void whenComparingSubsumptivePairs_binaryRelationBase_SubsumptionRelationHolds() throws ExecutionException, InterruptedException {
        testSubsumptionRelationHoldsBetweenPatternPairs(generateTestPairs(binaryRelationPatternTree, true).collect(Collectors.toList()), 4);
    }

    @Test
    public void whenComparingSubsumptivePairs_ternaryRelationBase_SubsumptionRelationHolds() throws ExecutionException, InterruptedException {
        testSubsumptionRelationHoldsBetweenPatternPairs(generateTestPairs(ternaryRelationPatternTree, false).collect(Collectors.toList()), 4);
    }

    @Test
    public void whenGeneralisingAttributes_SubsumptionRelationHoldsBetweenPairs(){
        int depth = 10;
        List<ReasonerQueryEquivalence> equivs = Lists.newArrayList(
                ReasonerQueryEquivalence.Equality,
                ReasonerQueryEquivalence.AlphaEquivalence,
                ReasonerQueryEquivalence.StructuralEquivalence);

        List<UnifierType> unifierTypes = Lists.newArrayList(
                UnifierType.EXACT,
                UnifierType.RULE,
                UnifierType.SUBSUMPTIVE,
                UnifierType.STRUCTURAL,
                UnifierType.STRUCTURAL_SUBSUMPTIVE
        );

        try (Transaction tx = genericSchemaSession.readTransaction()) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction) tx).reasonerQueryFactory();
            TransactionContext ctx = new TransactionContext(tx);

            Pattern input = Graql.and(Graql.var("x").has("resource-double", 16.0));
            Pattern input2 = Graql.and(Graql.var("x").has("resource-double", -16.0));

            List<Operator> ops = Lists.newArrayList(Operators.generaliseAttribute());

            HashMultimap<Pattern, Pattern> firstTree = new TarjanSCC<>(generatePatternTree(input, ctx, ops, depth)).successorMap();
            HashMultimap<Pattern, Pattern> secondTree = new TarjanSCC<>(generatePatternTree(input2, ctx, ops, depth)).successorMap();

            Operator fuzzer = Operators.fuzzVariables();

            firstTree.entries().forEach(e -> {
                ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(e.getKey()));

                Pattern childPattern = e.getValue();
                ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));

                QueryTestUtil.unification(parent, child, true, UnifierType.RULE);
                QueryTestUtil.unification(parent, child, true, UnifierType.SUBSUMPTIVE);
                QueryTestUtil.unification(parent, child, true, UnifierType.STRUCTURAL_SUBSUMPTIVE);
                equivs.forEach(equiv -> QueryTestUtil.queryEquivalence(parent, child, false, equiv));

                fuzzer.apply(parent.getPattern(), ctx).forEach(fuzzedParentPattern -> {
                            ReasonerAtomicQuery fuzzedParent = reasonerQueryFactory.atomic(conjunction(fuzzedParentPattern));
                            QueryTestUtil.queryEquivalence(parent, fuzzedParent, false, ReasonerQueryEquivalence.Equality);
                            QueryTestUtil.queryEquivalence(parent, fuzzedParent, true, ReasonerQueryEquivalence.AlphaEquivalence);
                            QueryTestUtil.queryEquivalence(parent, fuzzedParent, true, ReasonerQueryEquivalence.StructuralEquivalence);
                        });

                equivs.forEach(equiv -> QueryTestUtil.queryEquivalence(child, parent, false, equiv));

                secondTree.keySet().forEach(p -> {
                    ReasonerAtomicQuery unrelated = reasonerQueryFactory.atomic(conjunction(p));
                    if (!unrelated.getAtom().toAttributeAtom().isValueEquality()) {
                        fuzzer.apply(p, ctx).forEach(fuzzedUnrelatedPattern -> {
                            ReasonerAtomicQuery fuzzedUnrelated = reasonerQueryFactory.atomic(conjunction(fuzzedUnrelatedPattern));

                            Lists.newArrayList(unrelated, fuzzedUnrelated).forEach(unrel -> {
                                unifierTypes.forEach(unifierType -> {
                                    QueryTestUtil.unification(parent, unrel, false, unifierType);
                                    QueryTestUtil.unification(unrel, parent, false, unifierType);
                                    QueryTestUtil.unification(child, unrel, false, unifierType);
                                    QueryTestUtil.unification(unrel, child, false, unifierType);
                                });

                                equivs.forEach(equiv -> QueryTestUtil.queryEquivalence(child, unrel, false, equiv));
                                equivs.forEach(equiv -> QueryTestUtil.queryEquivalence(parent, unrel, false, equiv));
                            });
                        });
                    }
                });

            });
        }
    }

    @Test
    public void whenFuzzyingVariablesWithBindingsPreserved_AlphaEquivalenceIsNotAffected(){
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction) tx;
            ReasonerQueryFactory reasonerQueryFactory = testTx.reasonerQueryFactory();
            TransactionContext txCtx = new TransactionContext(tx);
            Set<Pattern> patterns = binaryRelationPatternTree.keySet();
            Operator fuzzer = Operators.fuzzVariables();
            for(Pattern pattern : patterns){
                List<Pattern> fuzzedPatterns = Stream.concat(Stream.of(pattern), fuzzer.apply(pattern, txCtx))
                        .flatMap(p -> Stream.concat(Stream.of(p), fuzzer.apply(p, txCtx)))
                        .collect(Collectors.toList());
                int N = fuzzedPatterns.size();
                for (int i = 0 ; i < N ;i++) {
                    Pattern p = fuzzedPatterns.get(i);
                    for (int j = i ; j < N ;j++) {
                        Pattern p2 = fuzzedPatterns.get(j);
                        ReasonerQueryImpl pQuery = reasonerQueryFactory.create(conjunction(p));
                        ReasonerQueryImpl cQuery = reasonerQueryFactory.create(conjunction(p2));

                        if (pQuery.isAtomic() && cQuery.isAtomic()) {
                            ReasonerAtomicQuery queryA = (ReasonerAtomicQuery) pQuery;
                            ReasonerAtomicQuery queryB = (ReasonerAtomicQuery) cQuery;
                            QueryTestUtil.unification(queryA, queryB,true, UnifierType.EXACT);
                            QueryTestUtil.queryEquivalence(queryA, queryB, i == j, ReasonerQueryEquivalence.Equality);
                            QueryTestUtil.queryEquivalence(queryA, queryB, true, ReasonerQueryEquivalence.StructuralEquivalence);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void whenFuzzyingIdsWithBindingsPreserved_StructuralEquivalenceIsNotAffected(){
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction) tx;
            ReasonerQueryFactory reasonerQueryFactory = testTx.reasonerQueryFactory();
            TransactionContext txCtx = new TransactionContext(tx);
            Set<Pattern> patterns = binaryRelationPatternTree.keySet();
            Operator fuzzer = Operators.fuzzIds();
            for(Pattern pattern : patterns){
                ReasonerQueryImpl pQuery = reasonerQueryFactory.create(conjunction(pattern));
                Stream.concat(Stream.of(pattern), fuzzer.apply(pattern, txCtx))
                        .flatMap(p -> Stream.concat(Stream.of(p), fuzzer.apply(p, txCtx)))
                        .forEach(fuzzedPattern -> {
                            ReasonerQueryImpl cQuery = reasonerQueryFactory.create(conjunction(fuzzedPattern));

                            if (pQuery.isAtomic() && cQuery.isAtomic()) {
                                ReasonerAtomicQuery queryA = (ReasonerAtomicQuery) pQuery;
                                ReasonerAtomicQuery queryB = (ReasonerAtomicQuery) cQuery;
                                QueryTestUtil.unification(queryA, queryB,true, UnifierType.STRUCTURAL);
                                QueryTestUtil.unification(queryA, queryB,true, UnifierType.STRUCTURAL_SUBSUMPTIVE);
                            }
                        });
                }
            }
    }

    private void testSubsumptionRelationHoldsBetweenPatternPairs(List<Pair<Pattern, Pattern>> testPairs, int threads) throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        int listSize = testPairs.size();
        int listChunk = listSize / threads + 1;

        List<CompletableFuture<Void>> testChunks = new ArrayList<>();
        for (int threadNo = 0; threadNo < threads; threadNo++) {
            boolean lastChunk = threadNo == threads - 1;
            final int startIndex = threadNo * listChunk;
            int endIndex = (threadNo + 1) * listChunk;
            if (endIndex > listSize && lastChunk) endIndex = listSize;

            List<Pair<Pattern, Pattern>> subList = testPairs.subList(startIndex, endIndex);
            System.out.println("Subset to test: " + subList.size());
            CompletableFuture<Void> testChunk = CompletableFuture.supplyAsync(() -> {
                try (Transaction tx = genericSchemaSession.readTransaction()) {
                    ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction) tx).reasonerQueryFactory();

                    for (Pair<Pattern, Pattern> pair : subList) {
                        ReasonerQueryImpl pQuery = reasonerQueryFactory.create(conjunction(pair.first()));
                        ReasonerQueryImpl cQuery = reasonerQueryFactory.create(conjunction(pair.second()));

                        if (pQuery.isAtomic() && cQuery.isAtomic()) {
                            ReasonerAtomicQuery parent = (ReasonerAtomicQuery) pQuery;
                            ReasonerAtomicQuery child = (ReasonerAtomicQuery) cQuery;

                            QueryTestUtil.unification(parent, child, true, UnifierType.RULE);
                            QueryTestUtil.unification(parent, child, true, UnifierType.SUBSUMPTIVE);
                            QueryTestUtil.unification(parent, child, true, UnifierType.STRUCTURAL_SUBSUMPTIVE);
                        }
                    }
                }
                return null;
            }, executorService);
            testChunks.add(testChunk);
        }
        CompletableFuture.allOf(testChunks.toArray(new CompletableFuture[]{})).get();
        executorService.shutdown();
    }

    private Conjunction<Statement> conjunction(Pattern pattern) {
        return Graql.and(pattern.statements());
    }
}
