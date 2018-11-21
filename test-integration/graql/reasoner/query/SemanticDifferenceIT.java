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

package grakn.core.graql.reasoner.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.graql.Graql;
import grakn.core.graql.admin.Conjunction;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.internal.pattern.Patterns;
import grakn.core.graql.internal.reasoner.atom.binary.ResourceAtom;
import grakn.core.graql.internal.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.internal.reasoner.cache.SemanticDifference;
import grakn.core.graql.internal.reasoner.cache.VariableDefinition;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.utils.Pair;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionImpl;
import java.util.HashSet;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class SemanticDifferenceIT {

    private static String resourcePath = "test-integration/graql/reasoner/resources/";
    
    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl genericSchemaSession;

    @BeforeClass
    public static void loadContext(){
        genericSchemaSession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit(resourcePath, "genericSchema.gql", genericSchemaSession);
    }

    @AfterClass
    public static void closeSession(){
        genericSchemaSession.close();
    }

    @Test
    public void typeSpecification(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            EntityType baseRoleEntity = tx.getEntityType("baseRoleEntity");
            String base = "(baseRole1: $x, baseRole2: $y) isa binary;";
            String generalPattern = patternise(base);
            String specificPattern = patternise(base, "$x isa " + baseRoleEntity.label() + ";");
            ReasonerAtomicQuery general = ReasonerQueries.atomic(conjunction(generalPattern), tx);
            ReasonerAtomicQuery specific = ReasonerQueries.atomic(conjunction(specificPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = specific.getMultiUnifierWithSemanticDiff(general);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableMap.of(
                            Graql.var("x"),
                            new VariableDefinition(baseRoleEntity, null, new HashSet<>(), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.getValue());
        }
    }

    @Test
    public void typeSpecialisation(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            EntityType baseRoleEntity = tx.getEntityType("baseRoleEntity");
            EntityType subRoleEntity = tx.getEntityType("subRoleEntity");
            String base = "(baseRole1: $x, baseRole2: $y) isa binary;";
            String generalPattern = patternise(base, "$x isa " + baseRoleEntity.label() + ";");
            String specificPattern = patternise(base, "$x isa " + subRoleEntity.label() + ";");
            ReasonerAtomicQuery general = ReasonerQueries.atomic(conjunction(generalPattern), tx);
            ReasonerAtomicQuery specific = ReasonerQueries.atomic(conjunction(specificPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = specific.getMultiUnifierWithSemanticDiff(general);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableMap.of(
                            Graql.var("x"),
                            new VariableDefinition(subRoleEntity, null, new HashSet<>(), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.getValue());
        }
    }

    @Test
    public void roleSpecification(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            Role role = tx.getRole("baseRole1");
            String base = "($role: $x, baseRole2: $y) isa binary;";
            String generalPattern = patternise(base);
            String specificPattern = patternise(base, "$role label " + role.label() + ";");
            ReasonerAtomicQuery general = ReasonerQueries.atomic(conjunction(generalPattern), tx);
            ReasonerAtomicQuery specific = ReasonerQueries.atomic(conjunction(specificPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = specific.getMultiUnifierWithSemanticDiff(general);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableMap.of(
                            Graql.var("role"),
                            new VariableDefinition(null, role, new HashSet<>(), new HashSet<>()),
                            Graql.var("x"),
                            new VariableDefinition(null, null, Sets.newHashSet(role), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.getValue());
        }
    }

    @Test
    public void roleSpecialisation(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            Role baseRole = tx.getRole("baseRole1");
            Role subRole = tx.getRole("subRole1");
            String base = "($role: $x, baseRole2: $y) isa binary;";
            String generalPattern = patternise(base, "$role label " + baseRole.label() + ";");
            String specificPattern = patternise(base, "$role label " + subRole.label() + ";");
            ReasonerAtomicQuery general = ReasonerQueries.atomic(conjunction(generalPattern), tx);
            ReasonerAtomicQuery specific = ReasonerQueries.atomic(conjunction(specificPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = specific.getMultiUnifierWithSemanticDiff(general);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableMap.of(
                            Graql.var("x"),
                            new VariableDefinition(null, null, Sets.newHashSet(subRole), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.getValue());
        }
    }

    @Test
    public void playedRoleSpecialisation(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            Role subRole1 = tx.getRole("subRole1");
            Role subRole2 = tx.getRole("subRole2");
            String generalPattern = patternise("(baseRole1: $x, baseRole2: $y) isa binary;");
            String specificPattern = patternise("(subRole1: $x, subRole2: $y) isa binary;");
            ReasonerAtomicQuery general = ReasonerQueries.atomic(conjunction(generalPattern), tx);
            ReasonerAtomicQuery specific = ReasonerQueries.atomic(conjunction(specificPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = specific.getMultiUnifierWithSemanticDiff(general);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableMap.of(
                            Graql.var("x"),
                            new VariableDefinition(null, null, Sets.newHashSet(subRole1), new HashSet<>()),
                            Graql.var("y"),
                            new VariableDefinition(null, null, Sets.newHashSet(subRole2), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.getValue());
        }
    }

    @Test
    public void valuePredicateSpecification(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            String generalPattern = patternise("$x has resource $r;");
            String specificPattern = patternise("$x has resource 'b';");
            ReasonerAtomicQuery general = ReasonerQueries.atomic(conjunction(generalPattern), tx);
            ReasonerAtomicQuery specific = ReasonerQueries.atomic(conjunction(specificPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = specific.getMultiUnifierWithSemanticDiff(general);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            ResourceAtom resource = (ResourceAtom) specific.getAtom();

            SemanticDifference expected = new SemanticDifference(
                    ImmutableMap.of(
                            resource.getAttributeVariable(),
                            new VariableDefinition(null, null, new HashSet<>(), resource.getInnerPredicates(ValuePredicate.class).collect(toSet()))
                    )
            );
            assertEquals(expected, semanticPair.getValue());
        }
    }

    //TODO needs more tests on subsumptive unification on resources
    @Ignore
    @Test
    public void valuePredicateSpecialisation(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            String generalPattern = patternise("$x has resource-long >0;");
            String specificPattern = patternise("$x has resource-long 1;");
            ReasonerAtomicQuery general = ReasonerQueries.atomic(conjunction(generalPattern), tx);
            ReasonerAtomicQuery specific = ReasonerQueries.atomic(conjunction(specificPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = specific.getMultiUnifierWithSemanticDiff(general);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            ResourceAtom resource = (ResourceAtom) specific.getAtom();

            SemanticDifference expected = new SemanticDifference(
                    ImmutableMap.of(
                            resource.getAttributeVariable(),
                            new VariableDefinition(null, null, new HashSet<>(), resource.getInnerPredicates(ValuePredicate.class).collect(toSet()))
                    )
            );
            assertEquals(expected, semanticPair.getValue());
        }
    }

    private String patternise(String... patterns){
        return "{" + String.join("", Sets.newHashSet(patterns)) + "}";
    }

    private Conjunction<VarPatternAdmin> conjunction(String patternString){
        Set<VarPatternAdmin> vars = Graql.parser().parsePattern(patternString).admin()
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }
}
