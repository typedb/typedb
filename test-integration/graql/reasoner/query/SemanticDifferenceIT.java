/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero parent Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero parent Public License for more details.
 *
 * You should have received a copy of the GNU Affero parent Public License
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
import org.junit.Test;

import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    public void whenChildSpecifiesType_typesAreFilteredCorrectly(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            EntityType subRoleEntity = tx.getEntityType("subRoleEntity");
            String base = "(baseRole1: $x, baseRole2: $y) isa binary;";
            String parentPattern = patternise(base);
            String childPattern = patternise(base, "$x isa " + subRoleEntity.label() + ";");
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = child.getMultiUnifierWithSemanticDiff(parent);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableMap.of(
                            Graql.var("x"),
                            new VariableDefinition(subRoleEntity, null, new HashSet<>(), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.getValue());

            parent.getQuery().stream()
                    .filter(expected::satisfiedBy)
                    .forEach(ans -> assertTrue(subRoleEntity.subs().anyMatch(t -> t.equals(ans.get("x").asThing().type()))));
        }
    }

    @Test
    public void whenChildSpecialisesType_typesAreFilteredCorrectly(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            EntityType baseRoleEntity = tx.getEntityType("baseRoleEntity");
            EntityType subRoleEntity = tx.getEntityType("subRoleEntity");
            String base = "(baseRole1: $x, baseRole2: $y) isa binary;";
            String parentPattern = patternise(base, "$x isa " + baseRoleEntity.label() + ";");
            String childPattern = patternise(base, "$x isa " + subRoleEntity.label() + ";");
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = child.getMultiUnifierWithSemanticDiff(parent);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableMap.of(
                            Graql.var("x"),
                            new VariableDefinition(subRoleEntity, null, new HashSet<>(), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.getValue());

            parent.getQuery().stream()
                    .filter(expected::satisfiedBy)
                    .forEach(ans -> assertTrue(subRoleEntity.subs().anyMatch(t -> t.equals(ans.get("x").asThing().type()))));
        }
    }

    @Test
    public void whenChildSpecifiesRole_rolesAreFilteredCorrectly(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            Role role = tx.getRole("baseRole1");
            String base = "($role: $x, baseRole2: $y) isa binary;";
            String parentPattern = patternise(base);
            String childPattern = patternise(base, "$role label " + role.label() + ";");
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = child.getMultiUnifierWithSemanticDiff(parent);
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

            parent.getQuery().stream()
                    .filter(expected::satisfiedBy)
                    .forEach(ans -> assertTrue(role.subs().anyMatch(t -> t.equals(ans.get("role")))));
        }
    }

    @Test
    public void whenChildSpecialisesRole_rolesAreFilteredCorrectly(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            Role baseRole = tx.getRole("baseRole1");
            Role subRole = tx.getRole("subRole1");
            String base = "$r ($role: $x, baseRole2: $y) isa binary;";
            String parentPattern = patternise(base, "$role label " + baseRole.label() + ";");
            String childPattern = patternise(base, "$role label " + subRole.label() + ";");
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = child.getMultiUnifierWithSemanticDiff(parent);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableMap.of(
                            Graql.var("role"),
                            new VariableDefinition(null, subRole, new HashSet<>(), new HashSet<>()),
                            Graql.var("x"),
                            new VariableDefinition(null, null, Sets.newHashSet(subRole), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.getValue());

            parent.getQuery().stream()
                    .filter(expected::satisfiedBy)
                    .forEach(ans -> assertTrue(subRole.subs().anyMatch(t -> t.equals(ans.get("role")))));
        }
    }

    @Test
    public void whenChildSpecialisesPlayedRole_RPsAreFilteredCorrectly(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            Role subRole1 = tx.getRole("subRole1");
            Role subRole2 = tx.getRole("subSubRole2");
            String parentPattern = patternise("(baseRole1: $x, baseRole2: $y) isa binary;");
            String childPattern = patternise("(" + subRole1.label() + ": $x, " + subRole2.label() + ": $y) isa binary;");
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = child.getMultiUnifierWithSemanticDiff(parent);
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

            parent.getQuery().stream()
                    .filter(expected::satisfiedBy)
                    .forEach(ans -> {
                        assertTrue(
                                !Sets.union(
                                        Sets.newHashSet(ans.get("x").asThing().relationships(subRole1)),
                                        Sets.newHashSet(ans.get("y").asThing().relationships(subRole2))
                        ).isEmpty()
                        );
                    });
        }
    }

    @Test
    public void whenChildSpecifiesValuePredicate_valuesAreFilteredCorrectly(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            String parentPattern = patternise("$x has resource-long $r;");
            final long value = 0;
            String childPattern = patternise("$x has resource-long " + value + ";");
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = child.getMultiUnifierWithSemanticDiff(parent);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            ResourceAtom resource = (ResourceAtom) child.getAtom();

            SemanticDifference expected = new SemanticDifference(
                    ImmutableMap.of(
                            resource.getAttributeVariable(),
                            new VariableDefinition(null, null, new HashSet<>(), resource.getInnerPredicates(ValuePredicate.class).collect(toSet()))
                    )
            );
            assertEquals(expected, semanticPair.getValue());

            parent.getQuery().stream()
                    .filter(expected::satisfiedBy)
                    .forEach(ans -> assertEquals(value, ans.get("r").asAttribute().value()));
        }
    }

    @Test
    public void valuePredicateSpecialisation(){
        try(TransactionImpl<?> tx = genericSchemaSession.transaction(Transaction.Type.WRITE)) {
            String parentPattern = patternise("$x has resource-long >0;");
            final long value = 1;
            String childPattern = patternise("$x has resource-long " + value + ";");
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = child.getMultiUnifierWithSemanticDiff(parent);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            ResourceAtom resource = (ResourceAtom) child.getAtom();

            SemanticDifference expected = new SemanticDifference(
                    ImmutableMap.of(
                            resource.getAttributeVariable(),
                            new VariableDefinition(null, null, new HashSet<>(), resource.getInnerPredicates(ValuePredicate.class).collect(toSet()))
                    )
            );
            assertEquals(expected, semanticPair.getValue());

            parent.getQuery().stream()
                    .filter(expected::satisfiedBy)
                    .forEach(ans -> assertEquals(value, ans.get("r").asAttribute().value()));
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
