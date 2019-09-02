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
 */

package grakn.core.graql.reasoner.query;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.Role;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.cache.SemanticDifference;
import grakn.core.graql.reasoner.cache.VariableDefinition;
import grakn.core.graql.reasoner.unifier.MultiUnifier;
import grakn.core.graql.reasoner.unifier.Unifier;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.graql.reasoner.utils.Pair;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static grakn.core.util.GraqlTestUtil.assertCollectionsEqual;
import static grakn.core.util.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static graql.lang.Graql.and;
import static graql.lang.Graql.neq;
import static graql.lang.Graql.val;
import static graql.lang.Graql.var;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class SemanticDifferenceIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl genericSchemaSession;

    @BeforeClass
    public static void loadContext(){
        genericSchemaSession = server.sessionWithNewKeyspace();
        String resourcePath = "test-integration/graql/reasoner/resources/";
        loadFromFileAndCommit(resourcePath, "genericSchema.gql", genericSchemaSession);
    }

    @AfterClass
    public static void closeSession(){
        genericSchemaSession.close();
    }

    @Test
    public void whenChildSpecifiesType_typesAreFilteredCorrectly(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().write()) {
            EntityType subRoleEntity = tx.getEntityType("subRoleEntity");

            Pattern parentPattern =
                    var().rel("baseRole1", var("z")).rel("baseRole2", var("w")).isa("binary");
            Pattern childPattern = and(
                    var().rel("baseRole1", var("x")).rel("baseRole2", var("y")).isa("binary"),
                    var("x").isa(subRoleEntity.label().getValue())
            );
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(new Variable("z"), subRoleEntity, null, new HashSet<>(), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.getValue());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(child, parent, semanticPair.getKey(), semanticPair.getValue());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildSpecialisesType_typesAreFilteredCorrectly(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().write()) {
            EntityType baseRoleEntity = tx.getEntityType("baseRoleEntity");
            EntityType subRoleEntity = tx.getEntityType("subRoleEntity");

            Pattern parentPattern = and(
                    var().rel("baseRole1", var("z")).rel("baseRole2", var("w")).isa("binary"),
                    var("z").isa(baseRoleEntity.label().getValue()));
            Pattern childPattern = and(
                    var().rel("baseRole1", var("x")).rel("baseRole2", var("y")).isa("binary"),
                    var("x").isa(subRoleEntity.label().getValue()));

            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(new Variable("z"), subRoleEntity, null, new HashSet<>(), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.getValue());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(child, parent, semanticPair.getKey(), semanticPair.getValue());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildGeneralisesType_semanticDifferenceIsTrivial(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().write()) {
            EntityType baseRoleEntity = tx.getEntityType("baseRoleEntity");
            EntityType metaEntityType = tx.getMetaEntityType();

            Pattern parentPattern = and(
                    var().rel("baseRole1", var("z")).rel("baseRole2", var("w")).isa("binary"),
                    var("z").isa(baseRoleEntity.label().getValue()));
            Pattern childPattern = and(
                    var().rel("baseRole1", var("x")).rel("baseRole2", var("y")).isa("binary"),
                    var("x").isa(metaEntityType.label().getValue()));

            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            MultiUnifier multiUnifier = parent.getMultiUnifier(child, UnifierType.SUBSUMPTIVE);
            multiUnifier.stream()
                    .map(u -> child.getAtom().semanticDifference(parent.getAtom(), u))
                    .forEach(sd -> assertTrue(sd.isTrivial()));
        }
    }

    @Test
    public void whenChildSpecifiesRole_rolesAreFilteredCorrectly(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().write()) {
            Role role = tx.getRole("baseRole1");
            Pattern parentPattern =
                    var().rel(var("role"), var("z")).rel("baseRole2", var("w")).isa("binary");
            Pattern childPattern = and(
                    var().rel(var("role"), var("x")).rel("baseRole2", var("y")).isa("binary"),
                    var("role").type(role.label().getValue()));

            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(new Variable("role"), null, role, new HashSet<>(), new HashSet<>()),
                            new VariableDefinition(new Variable("z"), null, null, Sets.newHashSet(role), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.getValue());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(child, parent, semanticPair.getKey(), semanticPair.getValue());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildSpecialisesRole_rolesAreFilteredCorrectly(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().write()) {
            Role baseRole = tx.getRole("baseRole1");
            Role subRole = tx.getRole("subRole1");

            Pattern parentPattern = and(
                    var().rel(var("role"), var("z")).rel("baseRole2", var("w")).isa("binary"),
                    var("role").type(baseRole.label().getValue()));
            Pattern childPattern = and(
                    var().rel(var("role"), var("x")).rel("baseRole2", var("y")).isa("binary"),
                    var("role").type(subRole.label().getValue()));
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(new Variable("z"), null, null, Sets.newHashSet(subRole), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.getValue());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(child, parent, semanticPair.getKey(), semanticPair.getValue());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildGeneralisesRole_semanticDifferenceIsTrivial(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().write()) {
            Role baseRole = tx.getRole("baseRole1");
            Role metaRole = tx.getMetaRole();

            Pattern parentPattern = and(
                    var().rel(var("role"), var("z")).rel("baseRole2", var("w")).isa("binary"),
                    var("role").type(baseRole.label().getValue()));
            Pattern childPattern = and(
                    var().rel(var("role"), var("x")).rel("baseRole2", var("y")).isa("binary"),
                    var("role").type(metaRole.label().getValue()));
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Unifier unifier = parent.getMultiUnifier(child, UnifierType.SUBSUMPTIVE).getUnifier();
            assertTrue(parent.getAtom().semanticDifference(child.getAtom(), unifier).isTrivial());
        }
    }

    @Test
    public void whenChildSpecialisesRole_rolePlayersPlayingMultipleRoles_differenceIsCalculatedCorrectly(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().write()) {
            Role baseRole1 = tx.getRole("baseRole1");
            Role subRole1 = tx.getRole("subRole1");
            Role baseRole2 = tx.getRole("baseRole2");
            Role subRole2 = tx.getRole("subRole2");

            Pattern parentPattern = and(
                    var().rel(var("role"), var("z")).rel(var("role2"), var("z")).isa("binary"),
                    var("role").type(baseRole1.label().getValue()),
                    var("role2").type(baseRole2.label().getValue()));
            Pattern childPattern = and(
                    var().rel(var("role"), var("x")).rel(var("role2"), var("x")).isa("binary"),
                    var("role").type(subRole1.label().getValue()),
                    var("role2").type(subRole2.label().getValue()));
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(new Variable("z"), null, null, Sets.newHashSet(subRole1, subRole2), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.getValue());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(child, parent, semanticPair.getKey(), semanticPair.getValue());
            assertCollectionsEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildAndParentHaveVariableRoles_differenceIsCalculatedCorrectly(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().write()) {
            Pattern parentPattern =
                    var().rel(var("role"), var("z")).rel(var("role2"), var("w")).isa("binary");
            Pattern childPattern = and(
                    var().rel(var("role"), var("x")).rel(var("role2"), var("y")).isa("binary"),
                    var("y").id("V123"));
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);

            SemanticDifference expected = new SemanticDifference(ImmutableSet.of());
            semanticPairs.stream().map(Pair::getValue).forEach(sd -> assertEquals(expected, sd));
        }
    }

    @Test
    public void whenChildSpecialisesPlayedRole_RPsAreFilteredCorrectly(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().write()) {
            Role subRole1 = tx.getRole("subRole1");
            Role subRole2 = tx.getRole("subSubRole2");
            Pattern parentPattern = var().rel("baseRole1", var("z")).rel("baseRole2", var("w")).isa("binary");
            Pattern childPattern = var().rel(subRole1.label().getValue(), var("x")).rel(subRole2.label().getValue(), var("y")).isa("binary");
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(new Variable("z"), null, null, Sets.newHashSet(subRole1), new HashSet<>()),
                            new VariableDefinition(new Variable("w"), null, null, Sets.newHashSet(subRole2), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.getValue());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(child, parent, semanticPair.getKey(), semanticPair.getValue());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildGeneralisesRoles_semanticDifferenceIsTrivial(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().write()) {
            Role metaRole = tx.getMetaRole();
            Pattern parentPattern = var().rel("baseRole1", var("z")).rel("baseRole2", var("w")).isa("binary");
            Pattern childPattern = var().rel(metaRole.label().getValue(), var("x")).rel(metaRole.label().getValue(), var("y")).isa("binary");

            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            MultiUnifier multiUnifier = parent.getMultiUnifier(child, UnifierType.SUBSUMPTIVE);
            multiUnifier.stream()
                    .map(u -> parent.getAtom().semanticDifference(child.getAtom(), u))
                    .forEach(semDiff -> assertTrue(semDiff.isTrivial()));
        }
    }

    @Test
    public void whenChildSpecifiesResourceValuePredicate_valuesAreFilteredCorrectly(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().write()) {
            final String value = "m";
            Pattern parentPattern = var("z").has("resource", var("r"));
            Pattern childPattern = var("x").has("resource", val(value));
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);
            Unifier unifier = semanticPair.getKey();

            AttributeAtom parentAtom = (AttributeAtom) parent.getAtom();
            Set<ValuePredicate> predicatesToSatisfy = child.getAtom().getInnerPredicates(ValuePredicate.class)
                    .flatMap(vp -> vp.unify(unifier.inverse()).stream())
                    .map(ValuePredicate.class::cast)
                    .collect(toSet());

            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(parentAtom.getAttributeVariable(),null, null, new HashSet<>(), predicatesToSatisfy)
                    )
            );
            assertEquals(expected, semanticPair.getValue());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(child, parent, semanticPair.getKey(), semanticPair.getValue());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildSpecialisesResourceValuePredicate_valuesAreFilteredCorrectly(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().write()) {
            final String value = "b";
            Pattern parentPattern = var("z").has("resource", neq("m"));
            Pattern childPattern = var("x").has("resource", val(value));
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);
            Unifier unifier = semanticPair.getKey();

            AttributeAtom parentAtom = (AttributeAtom) parent.getAtom();
            Set<ValuePredicate> predicatesToSatisfy = child.getAtom().getInnerPredicates(ValuePredicate.class)
                    .flatMap(vp -> vp.unify(unifier.inverse()).stream())
                    .collect(toSet());

            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(parentAtom.getAttributeVariable(),null, null, new HashSet<>(), predicatesToSatisfy)
                    )
            );
            assertEquals(expected, semanticPair.getValue());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(child, parent, semanticPair.getKey(), semanticPair.getValue());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildSpecifiesValuePredicateOnType_valuesAreFilteredCorrectly(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().write()) {
            final long value = 0;
            Pattern parentPattern = var("z").isa("resource-long");
            Pattern childPattern = var("x").isa("resource-long").val(value);
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);
            Unifier unifier = semanticPair.getKey();

            Set<ValuePredicate> predicatesToSatisfy = child.getAtom().getPredicates(ValuePredicate.class)
                    .flatMap(vp -> vp.unify(unifier.inverse()).stream())
                    .collect(toSet());
            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(parent.getAtom().getVarName(),null, null, new HashSet<>(), predicatesToSatisfy)
                    )
            );
            assertEquals(expected, semanticPair.getValue());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(child, parent, semanticPair.getKey(), semanticPair.getValue());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildSpecialisesValuePredicateOnType_valuesAreFilteredCorrectly2(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().write()) {
            final long value = 1;
            Pattern parentPattern = var("z").isa("resource-long").gt(0);
            Pattern childPattern = var("x").isa("resource-long").eq(value);
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);
            Unifier unifier = semanticPair.getKey();

            Set<ValuePredicate> predicatesToSatisfy = child.getAtom().getPredicates(ValuePredicate.class)
                    .flatMap(vp -> vp.unify(unifier.inverse()).stream())
                    .collect(toSet());
            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(parent.getAtom().getVarName(),null, null, new HashSet<>(), predicatesToSatisfy)
                    )
            );
            assertEquals(expected, semanticPair.getValue());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(child, parent, semanticPair.getKey(), semanticPair.getValue());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildGeneralisesValuePredicateOnType_semanticDifferenceIsTrivial(){
        try(TransactionOLTP tx = genericSchemaSession.transaction().write()) {
            final long value = 1;
            Pattern parentPattern = var("z").has("resource-long", value);
            Pattern childPattern = var("x").has("resource-long", var("r"));
            ReasonerAtomicQuery parent = ReasonerQueries.atomic(conjunction(parentPattern), tx);
            ReasonerAtomicQuery child = ReasonerQueries.atomic(conjunction(childPattern), tx);

            MultiUnifier multiUnifier = parent.getMultiUnifier(child, UnifierType.SUBSUMPTIVE);
            multiUnifier.stream()
                    .map(u -> parent.getAtom().semanticDifference(child.getAtom(), u))
                    .forEach(sd -> assertTrue(sd.isTrivial()));
        }
    }

    private Set<ConceptMap> projectAnswersToChild(ReasonerAtomicQuery child, ReasonerAtomicQuery parent, Unifier parentToChildUnifier, SemanticDifference diff){
        Set<Variable> childVars = child.getVarNames();
        ConceptMap childSub = child.getRoleSubstitution();
        return parent.tx().stream(parent.getQuery(), false)
                .map(ans -> diff.propagateAnswer(ans, childSub, childVars, parentToChildUnifier))
                .filter(ans -> !ans.isEmpty())
                .collect(Collectors.toSet());
    }

    private Conjunction<Statement> conjunction(Pattern pattern){
        Set<Statement> vars = pattern
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream())
                .collect(toSet());
        return and(vars);
    }
}
