/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

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
    private static HashMultimap<Pattern, Pattern> relationPatternTree;

    @BeforeClass
    public static void loadContext() {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        genericSchemaSession = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        String resourcePath = "test-integration/graql/reasoner/resources/";
        loadFromFileAndCommit(resourcePath, "genericSchema.gql", genericSchemaSession);

        try(Transaction tx = genericSchemaSession.readTransaction()) {
            String id = tx.getEntityType("baseRoleEntity").instances().iterator().next().id().getValue();
            String subId = tx.getEntityType("subRoleEntity").instances().iterator().next().id().getValue();
            Pattern basePattern = and(
                    var("r")
                            .rel("subRole1", var("x"))
                            .rel("subRole2", var("y"))
                            .isa("binary"),
                    var("x").isa("subRoleEntity"),
                    var("x").id(id),
                    var("y").isa("subRoleEntity"),
                    var("y").id(subId)
            );
            relationPatternTree = generatePatternTree(basePattern, new TransactionContext(tx));
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
    private static HashMultimap<Pattern, Pattern> generatePatternTree(Pattern basePattern, TransactionContext ctx){
        HashMultimap<Pattern, Pattern> patternTree = HashMultimap.create();

        Set<Pattern> output = Operators.removeSubstitution().apply(basePattern, ctx).collect(Collectors.toSet());
        List<Operator> ops = Lists.newArrayList(Operators.typeGeneralise(), Operators.roleGeneralise());

        while (!output.isEmpty()){
            Stream<Pattern> pstream = output.stream();
            for(Operator op : ops){
                pstream = pstream.flatMap(parent -> op.apply(parent, ctx).peek(child -> patternTree.put(parent, child)));
            }
            output = pstream.collect(Collectors.toSet());
        }
        return patternTree;
    }

    /**
     * Generates pattern pairs to test from the pregenerated pattern tree.
     * @param exhaustive whether to compute full transitive closure of the parent-child relation.
     * @return stream of test case pattern pairs
     */
    private static Stream<Pair<Pattern, Pattern>> generateTestPairs(boolean exhaustive){
        //non-exhaustive option returns only the direct parent-child pairs
        //instead of full transitive closure of the parent-child relation
        if (!exhaustive){
            return relationPatternTree.entries().stream().map(e -> new Pair<>(e.getKey(), e.getValue()));
        }

        TarjanSCC<Pattern> tarjan = new TarjanSCC<>(relationPatternTree);
        return tarjan.successorMap().entries().stream().map(e -> new Pair<>(e.getKey(), e.getValue()));
    }

    @Test
    public void whenComparingSubsumptivePairs_SubsumptionRelationHolds(){
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction) tx).reasonerQueryFactory();
            Iterator<Pair<Pattern, Pattern>> pairIterator = generateTestPairs(false).iterator();

            while(pairIterator.hasNext()){
                Pair<Pattern, Pattern> pair = pairIterator.next();

                ReasonerQueryImpl pQuery = reasonerQueryFactory.create(conjunction(pair.first()));
                ReasonerQueryImpl cQuery = reasonerQueryFactory.create(conjunction(pair.second()));

                if (pQuery.isAtomic() && cQuery.isAtomic()) {
                    ReasonerAtomicQuery parent = (ReasonerAtomicQuery) pQuery;
                    ReasonerAtomicQuery child = (ReasonerAtomicQuery) cQuery;

                    QueryTestUtil.unification(parent, child,true, UnifierType.RULE);
                    QueryTestUtil.unification(parent, child,true, UnifierType.SUBSUMPTIVE);
                    QueryTestUtil.unification(parent, child,true, UnifierType.STRUCTURAL_SUBSUMPTIVE);
                }
            }
        }
    }

    @Test
    public void whenFuzzyingVariablesWithBindingsPreserved_AlphaEquivalenceIsNotAffected(){
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction) tx;
            ReasonerQueryFactory reasonerQueryFactory = testTx.reasonerQueryFactory();
            TransactionContext txCtx = new TransactionContext(tx);
            Set<Pattern> patterns = relationPatternTree.keySet();
            Operator fuzzer = Operators.fuzzVariables();
            for(Pattern pattern : patterns){
                List<Pattern> fuzzedPatterns = Stream.concat(Stream.of(pattern), fuzzer.apply(pattern, txCtx))
                        .flatMap(p -> Stream.concat(Stream.of(p), fuzzer.apply(p, txCtx)))
                        .collect(Collectors.toList());
                int N = fuzzedPatterns.size();
                for (int i = 0 ; i < N ;i++) {
                    Pattern p = fuzzedPatterns.get(i);
                    fuzzedPatterns.subList(i, N).forEach(p2 -> {
                        ReasonerQueryImpl pQuery = reasonerQueryFactory.create(conjunction(p));
                        ReasonerQueryImpl cQuery = reasonerQueryFactory.create(conjunction(p2));

                        if (pQuery.isAtomic() && cQuery.isAtomic()) {
                            ReasonerAtomicQuery queryA = (ReasonerAtomicQuery) pQuery;
                            ReasonerAtomicQuery queryB = (ReasonerAtomicQuery) cQuery;
                            QueryTestUtil.unification(queryA, queryB,true, UnifierType.EXACT);
                        }
                    });
                }
            }
        }
    }

    private Conjunction<Statement> conjunction(Pattern pattern) {
        return Graql.and(pattern.statements());
    }
}
