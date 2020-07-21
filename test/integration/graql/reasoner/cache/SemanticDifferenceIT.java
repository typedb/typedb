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

package grakn.core.graql.reasoner.cache;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import grakn.common.util.Pair;
import grakn.core.common.config.Config;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestStorage;
import grakn.core.test.rule.SessionUtil;
import grakn.core.test.rule.TestTransactionProvider.TestTransaction;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static grakn.core.test.common.GraqlTestUtil.assertCollectionsEqual;
import static grakn.core.test.common.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static grakn.core.test.common.GraqlTestUtil.loadFromFileAndCommit;
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
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session genericSchemaSession;

    @BeforeClass
    public static void loadContext() {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        genericSchemaSession = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        String resourcePath = "test/integration/graql/reasoner/resources/";
        loadFromFileAndCommit(resourcePath, "genericSchemaRefactored.gql", genericSchemaSession);
    }

    @AfterClass
    public static void closeSession() {
        genericSchemaSession.close();
    }

    @Test
    public void whenChildSpecifiesType_typesAreFilteredCorrectly() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            EntityType subRoleEntity = tx.getEntityType("subRoleEntity");

            Pattern parentPattern =
                    var().rel("baseRole1", var("z")).rel("baseRole2", var("w")).isa("ternary");
            Pattern childPattern = and(
                    var().rel("baseRole1", var("x")).rel("baseRole2", var("y")).isa("ternary"),
                    var("x").isa(subRoleEntity.label().getValue())
            );
            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(new Variable("z"), subRoleEntity, null, new ArrayList<>(), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.second());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(tx, child, parent, semanticPair.first(), semanticPair.second());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildSpecialisesType_typesAreFilteredCorrectly() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            EntityType baseRoleEntity = tx.getEntityType("baseRoleEntity");
            EntityType subRoleEntity = tx.getEntityType("subRoleEntity");

            Pattern parentPattern = and(
                    var().rel("baseRole1", var("z")).rel("baseRole2", var("w")).isa("ternary"),
                    var("z").isa(baseRoleEntity.label().getValue()));
            Pattern childPattern = and(
                    var().rel("baseRole1", var("x")).rel("baseRole2", var("y")).isa("ternary"),
                    var("x").isa(subRoleEntity.label().getValue()));

            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(new Variable("z"), subRoleEntity, null, new ArrayList<>(), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.second());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(tx, child, parent, semanticPair.first(), semanticPair.second());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildGeneralisesType_semanticDifferenceIsTrivial() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            EntityType baseRoleEntity = tx.getEntityType("baseRoleEntity");
            EntityType metaEntityType = tx.getMetaEntityType();

            Pattern parentPattern = and(
                    var().rel("baseRole1", var("z")).rel("baseRole2", var("w")).isa("ternary"),
                    var("z").isa(baseRoleEntity.label().getValue()));
            Pattern childPattern = and(
                    var().rel("baseRole1", var("x")).rel("baseRole2", var("y")).isa("ternary"),
                    var("x").isa(metaEntityType.label().getValue()));

            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));

            MultiUnifier multiUnifier = parent.getMultiUnifier(child, UnifierType.SUBSUMPTIVE);
            multiUnifier.stream()
                    .map(u -> child.getAtom().computeSemanticDifference(parent.getAtom(), u))
                    .forEach(sd -> assertTrue(sd.isTrivial()));
        }
    }

    @Test
    public void whenChildSpecifiesRole_rolesAreFilteredCorrectly() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            Role role = tx.getRole("baseRole1");
            Pattern parentPattern =
                    var().rel(var("role"), var("z")).rel("baseRole2", var("w")).isa("ternary");
            Pattern childPattern = and(
                    var().rel(var("role"), var("x")).rel("baseRole2", var("y")).isa("ternary"),
                    var("role").type(role.label().getValue()));

            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(new Variable("role"), null, role, new ArrayList<>(), new HashSet<>()),
                            new VariableDefinition(new Variable("z"), null, null, Arrays.asList(role), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.second());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(tx, child, parent, semanticPair.first(), semanticPair.second());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildSpecialisesRole_rolesAreFilteredCorrectly() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            Role baseRole = tx.getRole("baseRole1");
            Role subRole = tx.getRole("subRole1");

            Pattern parentPattern = and(
                    var().rel(var("role"), var("z")).rel("baseRole2", var("w")).isa("ternary"),
                    var("role").type(baseRole.label().getValue()));
            Pattern childPattern = and(
                    var().rel(var("role"), var("x")).rel("baseRole2", var("y")).isa("ternary"),
                    var("role").type(subRole.label().getValue()));
            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(new Variable("z"), null, null, Arrays.asList(subRole), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.second());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(tx, child, parent, semanticPair.first(), semanticPair.second());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildSpecialisesRole_rolePlayersPlayingMultipleRoles_differenceIsCalculatedCorrectly() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            Role baseRole1 = tx.getRole("baseRole1");
            Role subRole1 = tx.getRole("subRole1");
            Role baseRole2 = tx.getRole("baseRole2");
            Role subRole2 = tx.getRole("subRole2");

            Pattern parentPattern = and(
                    var().rel(var("role"), var("z")).rel(var("role2"), var("z")).isa("ternary"),
                    var("role").type(baseRole1.label().getValue()),
                    var("role2").type(baseRole2.label().getValue()));
            Pattern childPattern = and(
                    var().rel(var("role"), var("x")).rel(var("role2"), var("x")).isa("ternary"),
                    var("role").type(subRole1.label().getValue()),
                    var("role2").type(subRole2.label().getValue()));
            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));

            // uses STRUCTURAL_SUBSUMPTIVE, so we have two different unifiers from role - role and role - role2 (the role vars are not distinct, so any combination possible)
            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);

            for (Pair<Unifier, SemanticDifference> semanticPair : semanticPairs) {
                SemanticDifference expected = new SemanticDifference(
                        ImmutableSet.of(
                                new VariableDefinition(new Variable("z"), null, null, Lists.newArrayList(subRole1, subRole2), new HashSet<>())
                        )
                );
                assertEquals(expected, semanticPair.second());
                Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
                Set<ConceptMap> propagatedAnswers = projectAnswersToChild(tx, child, parent, semanticPair.first(), semanticPair.second());
                assertCollectionsEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
            }
        }
    }

    @Test
    public void whenChildAndParentHaveVariableRoles_differenceIsCalculatedCorrectly() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            Pattern parentPattern =
                    var().rel(var("role"), var("z")).rel(var("role2"), var("w")).isa("ternary");
            Pattern childPattern = and(
                    var().rel(var("role"), var("x")).rel(var("role2"), var("y")).isa("ternary"),
                    var("y").id("V123"));
            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);

            SemanticDifference expected = new SemanticDifference(ImmutableSet.of());
            semanticPairs.stream().map(Pair::second).forEach(sd -> assertEquals(expected, sd));
        }
    }

    @Test
    public void whenChildSpecialisesPlayedRole_RPsAreFilteredCorrectly() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            Role subRole1 = tx.getRole("subRole1");
            Role subRole2 = tx.getRole("subSubRole2");
            Pattern parentPattern = var().rel("baseRole1", var("z")).rel("baseRole2", var("w")).isa("ternary");
            Pattern childPattern = var().rel(subRole1.label().getValue(), var("x")).rel(subRole2.label().getValue(), var("y")).isa("ternary");
            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(new Variable("z"), null, null, Arrays.asList(subRole1), new HashSet<>()),
                            new VariableDefinition(new Variable("w"), null, null, Arrays.asList(subRole2), new HashSet<>())
                    )
            );
            assertEquals(expected, semanticPair.second());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(tx, child, parent, semanticPair.first(), semanticPair.second());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildGeneralisesRoles_semanticDifferenceIsTrivial() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            Role metaRole = tx.getMetaRole();
            Pattern parentPattern = var().rel("baseRole1", var("z")).rel("baseRole2", var("w")).isa("ternary");
            Pattern childPattern = var().rel(metaRole.label().getValue(), var("x")).rel(metaRole.label().getValue(), var("y")).isa("ternary");

            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));

            MultiUnifier multiUnifier = parent.getMultiUnifier(child, UnifierType.SUBSUMPTIVE);
            multiUnifier.stream()
                    .map(u -> parent.getAtom().computeSemanticDifference(child.getAtom(), u))
                    .forEach(semDiff -> assertTrue(semDiff.isTrivial()));
        }
    }

    @Test
    public void whenChildSpecifiesResourceValuePredicate_valuesAreFilteredCorrectly() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            final String value = "m";
            Pattern parentPattern = var("z").has("resource", var("r"));
            Pattern childPattern = var("x").has("resource", val(value));
            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);
            Unifier unifier = semanticPair.first();

            AttributeAtom parentAtom = (AttributeAtom) parent.getAtom();
            Set<ValuePredicate> predicatesToSatisfy = child.getAtom().getInnerPredicates(ValuePredicate.class)
                    .flatMap(vp -> vp.unify(unifier.inverse()).stream())
                    .map(ValuePredicate.class::cast)
                    .collect(toSet());

            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(parentAtom.getAttributeVariable(), null, null, new ArrayList<>(), predicatesToSatisfy)
                    )
            );
            assertEquals(expected, semanticPair.second());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(tx, child, parent, semanticPair.first(), semanticPair.second());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildSpecialisesResourceValuePredicate_valuesAreFilteredCorrectly() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            final String value = "b";
            Pattern parentPattern = var("z").has("resource", neq("m"));
            Pattern childPattern = var("x").has("resource", val(value));
            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);
            Unifier unifier = semanticPair.first();

            AttributeAtom parentAtom = (AttributeAtom) parent.getAtom();
            Set<ValuePredicate> predicatesToSatisfy = child.getAtom().getInnerPredicates(ValuePredicate.class)
                    .flatMap(vp -> vp.unify(unifier.inverse()).stream())
                    .collect(toSet());

            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(parentAtom.getAttributeVariable(), null, null, new ArrayList<>(), predicatesToSatisfy)
                    )
            );
            assertEquals(expected, semanticPair.second());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(tx, child, parent, semanticPair.first(), semanticPair.second());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildSpecifiesValuePredicateOnType_valuesAreFilteredCorrectly() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            final long value = 0;
            Pattern parentPattern = var("z").isa("resource-long");
            Pattern childPattern = var("x").isa("resource-long").val(value);
            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);
            Unifier unifier = semanticPair.first();

            Set<ValuePredicate> predicatesToSatisfy = child.getAtom().getPredicates(ValuePredicate.class)
                    .flatMap(vp -> vp.unify(unifier.inverse()).stream())
                    .collect(toSet());
            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(parent.getAtom().getVarName(), null, null, new ArrayList<>(), predicatesToSatisfy)
                    )
            );
            assertEquals(expected, semanticPair.second());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(tx, child, parent, semanticPair.first(), semanticPair.second());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildSpecialisesValuePredicateOnType_valuesAreFilteredCorrectly2() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            final long value = 1;
            Pattern parentPattern = var("z").isa("resource-long").gt(0);
            Pattern childPattern = var("x").isa("resource-long").eq(value);
            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));

            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);
            Unifier unifier = semanticPair.first();

            Set<ValuePredicate> predicatesToSatisfy = child.getAtom().getPredicates(ValuePredicate.class)
                    .flatMap(vp -> vp.unify(unifier.inverse()).stream())
                    .collect(toSet());
            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(parent.getAtom().getVarName(), null, null, new ArrayList<>(), predicatesToSatisfy)
                    )
            );
            assertEquals(expected, semanticPair.second());
            Set<ConceptMap> childAnswers = tx.stream(child.getQuery(), false).collect(Collectors.toSet());
            Set<ConceptMap> propagatedAnswers = projectAnswersToChild(tx, child, parent, semanticPair.first(), semanticPair.second());
            assertCollectionsNonTriviallyEqual(propagatedAnswers + "\n!=\n" + childAnswers + "\n", childAnswers, propagatedAnswers);
        }
    }

    @Test
    public void whenChildGeneralisesValuePredicateOnType_semanticDifferenceIsTrivial() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            final long value = 1;
            Pattern parentPattern = var("z").has("resource-long", value);
            Pattern childPattern = var("x").has("resource-long", var("r"));
            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));

            MultiUnifier multiUnifier = parent.getMultiUnifier(child, UnifierType.SUBSUMPTIVE);
            multiUnifier.stream()
                    .map(u -> parent.getAtom().computeSemanticDifference(child.getAtom(), u))
                    .forEach(sd -> assertTrue(sd.isTrivial()));
        }
    }

    @Test
    public void whenChildSpecialisesWithDuplicateRole_differenceExists() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            Pattern childPattern = var().rel("baseRole1", var("x")).rel("baseRole1", var("x")).isa("ternary");
            Pattern parentPattern = var().rel("baseRole1", var("x")).isa("ternary");

            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));
            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            Role baseRole1 = tx.getRole("baseRole1");
            List<Role> expectedRoles = Arrays.asList(baseRole1, baseRole1);
            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(new Variable("x"), null, null, expectedRoles, new HashSet<>())
                    )
            );

            Unifier unifier = semanticPair.first();
            SemanticDifference diff = semanticPair.second();
            assertEquals(expected, diff);

            EntityType baseRoleEntity = tx.getEntityType("baseRoleEntity");
            Entity entity1 = baseRoleEntity.create();
            Entity entity2 = baseRoleEntity.create();

            Relation relation = tx.getRelationType("ternary").create();
            relation.assign(baseRole1, entity1).assign(baseRole1, entity2);

            ConceptMap answer = new ConceptMap(ImmutableMap.of(new Variable("x"), entity1));

            assertEquals(false, diff.satisfiedBy(answer));
        }
    }

    @Test
    public void whenChildSpecialisesWithQuadrupleRole_differenceExists() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            Pattern childPattern = var().rel("baseRole1", var("x")).rel("baseRole1", var("x")).rel("baseRole1", var("x")).rel("baseRole1", var("x")).isa("ternary");
            Pattern parentPattern = var().rel("baseRole1", var("x")).isa("ternary");

            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));
            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            Role baseRole1 = tx.getRole("baseRole1");

            List<Role> expectedRoles = Arrays.asList(baseRole1, baseRole1, baseRole1, baseRole1);
            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(new Variable("x"), null, null, expectedRoles, new HashSet<>())
                    )
            );

            Unifier unifier = semanticPair.first();
            SemanticDifference diff = semanticPair.second();
            assertEquals(expected, diff);

            EntityType baseRoleEntity = tx.getEntityType("baseRoleEntity");
            Entity entity1 = baseRoleEntity.create();
            Entity entity2 = baseRoleEntity.create();

            Relation relation = tx.getRelationType("ternary").create();
            relation.assign(baseRole1, entity1).assign(baseRole1, entity2);

            ConceptMap answer = new ConceptMap(ImmutableMap.of(new Variable("x"), entity1));
            assertEquals(false, diff.satisfiedBy(answer));
        }
    }


    @Test
    public void whenChildSpecialisesWithQuadrupleRole_differenceExistsAndIsSatisfiable() {
        try (TestTransaction tx = ((TestTransaction) genericSchemaSession.transaction(Transaction.Type.WRITE))) {
            ReasonerQueryFactory reasonerQueryFactory = tx.reasonerQueryFactory();
            Pattern childPattern = var().rel("baseRole1", var("x")).rel("baseRole1", var("x")).rel("baseRole1", var("x")).rel("baseRole1", var("x")).isa("ternary");
            Pattern parentPattern = var().rel("baseRole1", var("x")).isa("ternary");

            ReasonerAtomicQuery parent = reasonerQueryFactory.atomic(conjunction(parentPattern));
            ReasonerAtomicQuery child = reasonerQueryFactory.atomic(conjunction(childPattern));
            Set<Pair<Unifier, SemanticDifference>> semanticPairs = parent.getMultiUnifierWithSemanticDiff(child);
            Pair<Unifier, SemanticDifference> semanticPair = Iterables.getOnlyElement(semanticPairs);

            Role baseRole1 = tx.getRole("baseRole1");

            List<Role> expectedRoles = Arrays.asList(baseRole1, baseRole1, baseRole1, baseRole1);
            SemanticDifference expected = new SemanticDifference(
                    ImmutableSet.of(
                            new VariableDefinition(new Variable("x"), null, null, expectedRoles, new HashSet<>())
                    )
            );

            Unifier unifier = semanticPair.first();
            SemanticDifference diff = semanticPair.second();
            assertEquals(expected, diff);

            EntityType baseRoleEntity = tx.getEntityType("baseRoleEntity");
            Entity entity1 = baseRoleEntity.create();

            Relation relation = tx.getRelationType("ternary").create();
            relation.assign(baseRole1, entity1).assign(baseRole1, entity1).assign(baseRole1, entity1).assign(baseRole1, entity1);

            ConceptMap answer = new ConceptMap(ImmutableMap.of(new Variable("x"), entity1));
            assertEquals(true, diff.satisfiedBy(answer));
        }
    }


    private Set<ConceptMap> projectAnswersToChild(Transaction tx, ReasonerAtomicQuery child, ReasonerAtomicQuery parent, Unifier parentToChildUnifier, SemanticDifference diff) {
        Set<Variable> childVars = child.getVarNames();
        ConceptMap childSub = child.getRoleSubstitution();
        return tx.stream(parent.getQuery(), false)
                .map(ans -> diff.propagateAnswer(ans, childSub, childVars, parentToChildUnifier))
                .filter(ans -> !ans.isEmpty())
                .collect(Collectors.toSet());
    }

    private Conjunction<Statement> conjunction(Pattern pattern) {
        Set<Statement> vars = pattern
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream())
                .collect(toSet());
        return and(vars);
    }
}
