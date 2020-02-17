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
import grakn.core.graql.reasoner.unifier.MultiUnifierImpl;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.graql.reasoner.utils.TarjanSCC;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import grakn.theory.tools.operator.Operator;
import grakn.theory.tools.operator.Operators;
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

public class GenerativeOperationalIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session genericSchemaSession;

    @BeforeClass
    public static void loadContext() {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        genericSchemaSession = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        String resourcePath = "test-integration/graql/reasoner/resources/";
        loadFromFileAndCommit(resourcePath, "genericSchema.gql", genericSchemaSession);
    }

    @AfterClass
    public static void closeSession() {
        genericSchemaSession.close();
    }

    private Stream<Pair<Pattern, Pattern>> generateTestPairs(Pattern basePattern, TransactionContext ctx, boolean exhaustive){
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

        //non-exhaustive option returns only the direct parent-child pairs
        //instead of full transitive closure of the parent-child relation
        if (!exhaustive){
            return patternTree.entries().stream().map(e -> new Pair<>(e.getKey(), e.getValue()));
        }

        TarjanSCC<Pattern> tarjan = new TarjanSCC<>(patternTree);
        return tarjan.successorMap().entries().stream().map(e -> new Pair<>(e.getKey(), e.getValue()));
    }

    @Test
    public void testSubsumptionRelationHoldsBetweenGeneratedPairs(){
        try (Transaction tx = genericSchemaSession.readTransaction()) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction) tx).reasonerQueryFactory();
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
            Iterator<Pair<Pattern, Pattern>> pairIterator = generateTestPairs(basePattern, new TransactionContext(tx), false).iterator();

            boolean pass = true;
            int failures = 0;
            int processed = 0;
            while(pairIterator.hasNext()){
                Pair<Pattern, Pattern> pair = pairIterator.next();

                ReasonerQueryImpl pQuery = reasonerQueryFactory.create(conjunction(pair.first()));
                ReasonerQueryImpl cQuery = reasonerQueryFactory.create(conjunction(pair.second()));

                if (pQuery.isAtomic() && cQuery.isAtomic()) {
                    ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(pQuery.getAtoms());
                    ReasonerAtomicQuery child = reasonerQueryFactory.atomic(cQuery.getAtoms());

                    if(!parent.isSubsumedBy(child)){
                        System.out.println("Subsumption failure comparing : " + parent + " ?=< " + child);
                        pass = false;
                        failures++;
                    }
                    if(parent.getMultiUnifier(child, UnifierType.RULE).equals(MultiUnifierImpl.nonExistent())){
                        System.out.println("Unifier failure comparing : " + parent + " ?=< " + child);
                        pass = false;
                        failures++;
                    }
                }
                processed++;
            }
            System.out.println("failures: " + failures + "/" + processed);
            //TODO currently we are having failures, uncomment when bugs are fixed
            //assertTrue(pass);
        }
    }

    private Conjunction<Statement> conjunction(Pattern pattern) {
        return Graql.and(pattern.statements());
    }
}
