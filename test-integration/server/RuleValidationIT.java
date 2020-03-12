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

package grakn.core.server;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import grakn.core.common.config.Config;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.core.Schema;
import grakn.core.graql.reasoner.rule.InferenceRule;
import grakn.core.graql.reasoner.rule.RuleUtils;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.InvalidKBException;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RuleValidationIT {
    @org.junit.Rule
    public final ExpectedException expectedException = ExpectedException.none();


    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private Session session;

    @Before
    public void setUp() {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        session = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
    }

    @After
    public void tearDown() {
        session.close();
    }

    @Test
    public void whenCreatingRulesWithNullValues_Throw() throws NullPointerException {
        try (Transaction tx = session.writeTransaction()) {
            expectedException.expect(NullPointerException.class);
            tx.putRule("A Thing", null, null);
        }
    }

    @Test
    public void whenCreatingRulesWithNonExistentEntityType_Throw() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            tx.putEntityType("My-Type");

            Pattern when = Graql.parsePattern("$x isa Your-Type;");
            Pattern then = Graql.parsePattern("$x isa My-Type;");
            Rule rule = tx.putRule("My-Sad-Rule-Type", when, then);

            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(
                    ErrorMessage.VALIDATION_RULE_MISSING_ELEMENTS.getMessage(Schema.VertexProperty.RULE_WHEN.name(), rule.label(), "Your-Type"));

            tx.commit();
        }
    }

    @Test
    public void whenCreatingDistinctRulesWithSimilarStringHashes_EnsureRulesDoNotClash() {
        try (Transaction tx = session.writeTransaction()) {
            String productRefused = "productRefused";
            Pattern when1 = Graql.parsePattern("{$step has step-id 9; $e (process-case: $case) isa process-record; $case has consent false;};");
            Pattern then1 = Graql.parsePattern("{(record: $e, step: $step) isa record-step;};");
            Rule rule1 = tx.putRule(productRefused, when1, then1);

            String productAccepted = "productAccepted";
            Pattern when2 = Graql.parsePattern("{$step has step-id 7; $e (process-case: $case) isa process-record; $case has consent true;};");
            Pattern then2 = Graql.parsePattern("{(record: $e, step: $step) isa record-step;};");
            Rule rule2 = tx.putRule(productAccepted, when2, then2);

            assertEquals(rule1, tx.getRule(productRefused));
            assertEquals(rule2, tx.getRule(productAccepted));
        }
    }

    @Test
    public void whenAddingRuleWithDisjunctionInTheBody_Throw() throws InvalidKBException {
        validateIllegalRule(
                Graql.parsePattern("{(role: $x);} or {(role: $x, role: $y);};"),
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_BODY
        );
    }

    @Test
    public void whenAddingRuleWithDisjunctionInTheHead_Throw() throws InvalidKBException {
        validateIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("{(someRole: $x);} or {(someRole: $x, anotherRole: $y);};"),
                ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleWithNonAtomicHead_Throw() throws InvalidKBException {
        validateIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("{(someRole: $x, anotherRole: $y) isa someRelation; $x has someAttribute 100;};"),
                ErrorMessage.VALIDATION_RULE_HEAD_NON_ATOMIC
        );
        validateIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("{someRole: $x, anotherRole: $y) isa someRelation; (someRole: $y, anotherRole: $z) isa someRelation;};"),
                ErrorMessage.VALIDATION_RULE_HEAD_NON_ATOMIC
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithUnboundRolePlayer_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("(someRole: $y, anotherRole: $z) isa someRelation;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithUnboundRelationVariable_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("{" +
                        "$r1 (someRole: $x, anotherRole: $y) isa someRelation;" +
                        "$r2 (someRole: $z, anotherRole: $u) isa someRelation;" +
                        "};"),
                Graql.parsePattern("$r (someRole: $r1, anotherRole: $r2) isa someRelation;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithUnboundVariablePredicate_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("$x has someAttribute $r;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_IsaAtomWithoutType_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("{$x isa $z;};"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_AMBIGUOUS_SCHEMA_CONCEPT
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithInequality_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("$x has someAttribute >10;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATTRIBUTE_WITH_NONSPECIFIC_PREDICATE
        );
    }

    @Test
    public void whenAddingARuleThatCopiesValuesBetweenCompatibleAttributes_DoNotThrow() {
        validateLegalHead(
                Graql.parsePattern("$x has someAttribute $r;"),
                Graql.parsePattern("$x has anotherAttribute $r;")
        );
    }

    @Test
    public void whenAddingARuleThatCopiesValuesBetweenIncompatibleAttributes_Throw() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            initTx(tx);
            Rule rule = tx.putRule(
                    UUID.randomUUID().toString(),
                    Graql.parsePattern("$x has stringAttribute $r;"),
                    Graql.parsePattern("$x has anotherAttribute $r;")
            );

            ErrorMessage message = ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_COPYING_INCOMPATIBLE_ATTRIBUTE_VALUES;
            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(
                    message.getMessage(
                            tx.getSchemaConcept(Label.of("anotherAttribute")).label(),
                            rule.label(),
                            tx.getSchemaConcept(Label.of("stringAttribute")).label())
            );
            tx.commit();
        }
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithAmbiguousPredicates_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("{$x has someAttribute $r; $r == 10; $r == 20;};"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATTRIBUTE_WITH_AMBIGUOUS_PREDICATES
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithMetaRoles_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("(role: $y, role: $x) isa someRelation;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithMissingRoles_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("($x, $y) isa someRelation;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithVariableRoles_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("($r1: $x, $r2: $y) isa someRelation;"),
                Graql.parsePattern("($r2: $x, $r1: $y) isa someRelation;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithoutType_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("(singleRole: $y, singleRole: $x);"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_AMBIGUOUS_SCHEMA_CONCEPT
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithImplicitType_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("($x, $y) isa " + Schema.ImplicitType.HAS.getLabel("someAttribute;").getValue()),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_IMPLICIT_SCHEMA_CONCEPT
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithImplicitRole_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("(" + Schema.ImplicitType.HAS_OWNER.getLabel("someAttribute").getValue() + ": $x, $y);"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_IMPLICIT_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_IllegalTypeAtoms_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("$x sub someEntity;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("$x plays someRole;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("$x isa someRelation;"),
                Graql.parsePattern("$x relates someRole;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("$x has someAttribute;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_Predicate_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("$x id V123;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("$x != $y';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("($x, $y); $x isa someAttribute;"),
                Graql.parsePattern("$x == '100';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("$x != $y';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalRule(
                Graql.parsePattern("($x, $y); $x isa someAttribute;"),
                Graql.parsePattern("$x == '100';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("$x label 'someEntity';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_PropertyAtoms_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("$x abstract;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("$x has someAttribute $y;"),
                Graql.parsePattern("$y datatype string;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("$x isa someEntity;"),
                Graql.parsePattern("$x regex /entity/;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_RelationDoesntRelateARole_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(someRole: $x, singleRole: $y) isa anotherRelation;"),
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED,
                "someRole", "anotherRelation"
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("(someRole: $x, singleRole: $y) isa anotherRelation;"),
                ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED,
                "someRole", "anotherRelation"
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_TypeCantPlayARole_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("{$x isa someEntity;$y isa someEntity; };"),
                Graql.parsePattern("(singleRole: $x, singleRole: $y) isa anotherRelation;"),
                ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE,
                "someEntity", "singleRole", "anotherRelation"
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("{$y isa someEntity; (singleRole: $x, singleRole: $y) isa anotherRelation;};"),
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE,
                "someEntity", "singleRole", "anotherRelation"
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("{(someRole: $x, anotherRole: $y) isa someRelation;$y isa someEntity;};"),
                Graql.parsePattern("{(singleRole: $x, singleRole: $y) isa anotherRelation;};"),
                ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE,
                "someEntity", "singleRole", "anotherRelation"
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_EntityCantHaveResource_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("$x isa someRelation;"),
                Graql.parsePattern("$x has someAttribute 1337;"),
                ErrorMessage.VALIDATION_RULE_ATTRIBUTE_OWNER_CANNOT_HAVE_ATTRIBUTE,
                "someAttribute", "someRelation"
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("{$x isa someRelation, has someAttribute 1337;};"),
                Graql.parsePattern("$x isa anotherRelation;"),
                ErrorMessage.VALIDATION_RULE_ATTRIBUTE_OWNER_CANNOT_HAVE_ATTRIBUTE,
                "someAttribute", "someRelation"
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("$x isa anotherRelation;"),
                Graql.parsePattern("{$x isa someRelation, has someAttribute 1337;"),
                ErrorMessage.VALIDATION_RULE_ATTRIBUTE_OWNER_CANNOT_HAVE_ATTRIBUTE,
                "someAttribute", "someRelation"
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_RelationWithInvalidType_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(someRole: $x, singleRole: $y) isa someAttribute;"),
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE,
                "someAttribute"
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("(someRole: $x, singleRole: $y) isa someAttribute;"),
                ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE,
                "someAttribute"
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_ResourceWithInvalidType_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("$x has someRelation $r;"),
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                ErrorMessage.VALIDATION_RULE_INVALID_ATTRIBUTE_TYPE,
                "someRelation"
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("{$r isa attribute; (someRole: $x, anotherRole: $y) isa someRelation;}"),
                Graql.parsePattern("$x has someRelation $r;"),
                ErrorMessage.VALIDATION_RULE_INVALID_ATTRIBUTE_TYPE,
                "someRelation"
        );
    }

    @Test
    public void whenAddingRuleWithOntologicallyInvalidHead_RelationDoesntRelateARole_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa someRelation;"),
                Graql.parsePattern("(someRole: $x, singleRole: $y) isa anotherRelation;"),
                ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED,
                "someRole", "anotherRelation"
        );
    }

    @Test
    public void whenAddingASimpleRuleWithNegationCycle_Throw() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            Pattern when = Graql.parsePattern(
                    "{" +
                            "$x isa entity;" +
                            "not {$y isa someEntity;};" +
                            "($x, $y) isa someRelation;" +
                            "};");
            Pattern then = Graql.parsePattern("$x isa someEntity;");

            initTx(tx);
            tx.putRule(UUID.randomUUID().toString(), when, then);
            List<Set<Type>> cycles = new ArrayList<>();
            cycles.add(Collections.singleton(tx.getEntityType("someEntity")));

            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(ErrorMessage.VALIDATION_RULE_GRAPH_NOT_STRATIFIABLE.getMessage(cycles));
            tx.commit();
        }
    }

    @Test
    public void whenAddingNonStratifiableRules_Throw() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            EntityType p = tx.putEntityType("p");
            EntityType q = tx.putEntityType("q");
            EntityType r = tx.putEntityType("r");

            tx.putRule(UUID.randomUUID().toString(),
                    Graql.parsePattern("$x isa p;"),
                    Graql.parsePattern("$x isa q;"));

            tx.putRule(UUID.randomUUID().toString(),
                    Graql.parsePattern("$x isa q;"),
                    Graql.parsePattern("$x isa r;"));

            tx.putRule(UUID.randomUUID().toString(),
                    Graql.parsePattern("{$x isa entity;not{ $x isa r;};};"),
                    Graql.parsePattern("$x isa p;"));

            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(allOf(
                    containsString("The rule graph is not stratifiable"),
                    containsString(p.toString()),
                    containsString(q.toString()),
                    containsString(r.toString()))
            );
            tx.commit();
        }
    }

    @Test
    public void whenAdditionOfRuleWithMetaTypeMakesRulesNonstratifiable_Throw() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            tx.putEntityType("p");
            tx.putEntityType("q");
            tx.putEntityType("r");

            tx.putRule(UUID.randomUUID().toString(),
                    Graql.parsePattern("$x isa p;"),
                    Graql.parsePattern("$x isa q;"));

            tx.putRule(UUID.randomUUID().toString(),
                    Graql.parsePattern("$x isa q;"),
                    Graql.parsePattern("$x isa r;"));

            tx.commit();
        }

        try (Transaction tx = session.writeTransaction()) {
            EntityType p = tx.putEntityType("p");
            EntityType q = tx.putEntityType("q");
            EntityType r = tx.putEntityType("r");

            tx.putRule(UUID.randomUUID().toString(),
                    Graql.parsePattern("{$x isa entity;not{ $x isa r;};};"),
                    Graql.parsePattern("$x isa p;"));

            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(allOf(
                    containsString("The rule graph is not stratifiable"),
                    containsString(p.toString()),
                    containsString(q.toString()),
                    containsString(r.toString()))
            );

            tx.commit();
        }
    }

    @Test
    public void whenAdditionOfRuleMakesRulesNonstratifiable_Throw() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            tx.putEntityType("p");
            tx.putEntityType("q");
            tx.putEntityType("r");

            tx.putRule(UUID.randomUUID().toString(),
                    Graql.parsePattern("$x isa p;"),
                    Graql.parsePattern("$x isa q;"));

            tx.putRule(UUID.randomUUID().toString(),
                    Graql.parsePattern("$x isa q;"),
                    Graql.parsePattern("$x isa r;"));

            tx.commit();
        }

        try (Transaction tx = session.writeTransaction()) {
            EntityType p = tx.putEntityType("p");
            EntityType q = tx.putEntityType("q");
            EntityType r = tx.putEntityType("r");
            EntityType z = tx.putEntityType("z");

            tx.putRule(UUID.randomUUID().toString(),
                    Graql.parsePattern("{$x isa z;not{ $x isa r;};};"),
                    Graql.parsePattern("$x isa p;"));

            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(allOf(
                    containsString("The rule graph is not stratifiable"),
                    containsString(p.toString()),
                    containsString(q.toString()),
                    containsString(r.toString()))
            );

            tx.commit();
        }
    }

    @Test
    public void whenAddingAddingStratifiableRules_correctStratificationIsProduced() {
        try (Transaction tx = session.writeTransaction()) {
            EntityType p = tx.putEntityType("p");
            EntityType q = tx.putEntityType("q");
            EntityType r = tx.putEntityType("r");
            EntityType s = tx.putEntityType("s");
            EntityType t = tx.putEntityType("t");
            EntityType u = tx.putEntityType("u");

            Rule Rp1 = tx.putRule("Rp1",
                    Graql.parsePattern("{$x isa q; not{ $x isa r;};};"),
                    Graql.parsePattern("$x isa p;"));

            Rule Rp2 = tx.putRule("Rp2",
                    Graql.parsePattern("{$x isa q; not{ $x isa t;};};"),
                    Graql.parsePattern("$x isa p;"));

            Rule Rr = tx.putRule("Rr",
                    Graql.parsePattern("{$x isa s; not{ $x isa t;};};"),
                    Graql.parsePattern("$x isa r;"));

            Rule Rs = tx.putRule("Rs",
                    Graql.parsePattern("$x isa u;"),
                    Graql.parsePattern("$x isa s;"));

            tx.commit();
        }
        try (Transaction tx = session.writeTransaction()) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction) tx;
            List<Rule> rules = RuleUtils.stratifyRules(
                    testTx.ruleCache().getRules()
                            .map(rule -> new InferenceRule(rule, testTx.reasonerQueryFactory()))
                            .collect(Collectors.toSet()))
                    .map(InferenceRule::getRule).collect(Collectors.toList());
            Rule Rp1 = tx.getRule("Rp1");
            Rule Rp2 = tx.getRule("Rp2");
            Rule Rr = tx.getRule("Rr");
            Rule Rs = tx.getRule("Rs");
            List<Rule> expected1 = Lists.newArrayList(Rs, Rr, Rp1, Rp2);
            List<Rule> expected2 = Lists.newArrayList(Rs, Rr, Rp2, Rp1);
            assertTrue(rules.equals(expected1) || rules.equals(expected2));
            expected1.forEach(Concept::delete);
            tx.commit();
        }
    }

    @Test
    public void whenAddingARuleWithMultipleNegationBlocks_Throw() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            Pattern when = Graql.parsePattern(
                    "{" +
                            "$x isa thing;" +
                            "not {$x isa someRelation;};" +
                            "not {$y isa attribute;};" +
                            "};");
            Pattern then = Graql.parsePattern("$x isa someEntity;");

            initTx(tx);
            Rule rule = tx.putRule(UUID.randomUUID().toString(), when, then);
            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(ErrorMessage.VALIDATION_RULE_MULTIPLE_NEGATION_BLOCKS.getMessage(rule.label()));
            tx.commit();
        }
    }

    @Test
    public void whenAddingARuleWithNestedNegationBlock_Throw() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            Pattern when = Graql.parsePattern(
                    "{" +
                            "$x isa entity;" +
                            "not {" +
                            "$y isa someRelation;" +
                            "not {$y isa attribute;};" +
                            "};" +
                            "($x, $y) isa someRelation;" +
                            "};");
            Pattern then = Graql.parsePattern("$x isa someEntity;");

            initTx(tx);
            Rule rule = tx.putRule(UUID.randomUUID().toString(), when, then);
            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(ErrorMessage.VALIDATION_RULE_NESTED_NEGATION.getMessage(rule.label()));
            tx.commit();
        }
    }

    @Test
    public void whenAddingARuleWithDisjunctiveNegationBlock_Throw() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            Pattern when = Graql.parsePattern(
                    "{" +
                            "$x isa entity;" +
                            "($x, $y) isa someRelation;" +
                            "not {" +
                            "{$y isa someRelation;} or " +
                            "{$y isa attribute;};" +
                            "};" +
                            "};");
            Pattern then = Graql.parsePattern("$x isa someEntity;");

            initTx(tx);
            Rule rule = tx.putRule(UUID.randomUUID().toString(), when, then);
            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(
                    ErrorMessage.VALIDATION_RULE_INVALID.getMessage(rule.label(), ErrorMessage.DISJUNCTIVE_NEGATION_BLOCK.getMessage())
            );
            tx.commit();
        }
    }

    @Test
    public void whenCreatingRules_EnsureHypothesisAndConclusionTypesAreFilledOnCommit() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            tx.putEntityType("someType");
            tx.putEntityType("anotherType");

            Pattern when = Graql.parsePattern("$x isa someType;");
            Pattern then = Graql.parsePattern("$x isa anotherType;");

            Rule rule = tx.putRule("My-Happy-Rule", when, then);
            assertThat(rule.whenTypes().collect(Collectors.toSet()), empty());
            assertThat(rule.thenTypes().collect(Collectors.toSet()), empty());

            tx.commit();
        }
        Transaction tx = session.readTransaction();
        EntityType t1 = tx.getEntityType("someType");
        EntityType t2 = tx.getEntityType("anotherType");
        Rule rule = tx.getRule("My-Happy-Rule");

        assertThat(rule.whenTypes().collect(Collectors.toSet()), containsInAnyOrder(t1));
        assertThat(rule.thenTypes().collect(Collectors.toSet()), containsInAnyOrder(t2));
        tx.close();
    }

    @Test
    public void whenCreatingRules_EnsureWhenTypesAndSignAreLinkedCorrectlyOnCommit() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            tx.putEntityType("p");
            tx.putEntityType("q");
            tx.putEntityType("r");
            tx.putEntityType("s");
            tx.putEntityType("t");

            Rule rule = tx.putRule("Rule",
                    Graql.parsePattern("{" +
                            "$x isa p;" +
                            "$x isa q;" +
                            "not{ " +
                            "$x isa r;" +
                            "$x isa s;" +
                            "};" +
                            "};"),
                    Graql.parsePattern("$x isa t;"));

            assertThat(rule.whenTypes().collect(Collectors.toSet()), empty());
            assertThat(rule.thenTypes().collect(Collectors.toSet()), empty());

            tx.commit();
        }
        Transaction tx = session.readTransaction();
        Type p = tx.getEntityType("p");
        Type q = tx.getEntityType("q");
        Type r = tx.getEntityType("r");
        Type s = tx.getEntityType("s");
        Type t = tx.getEntityType("t");
        Rule rule = tx.getRule("Rule");

        assertEquals(rule.whenTypes().collect(Collectors.toSet()), Sets.newHashSet(p, q, r, s));
        assertEquals(rule.whenPositiveTypes().collect(Collectors.toSet()), Sets.newHashSet(p, q));
        assertEquals(rule.whenNegativeTypes().collect(Collectors.toSet()), Sets.newHashSet(r, s));
        assertThat(rule.thenTypes().collect(Collectors.toSet()), containsInAnyOrder(t));
        tx.close();
    }

    @Test
    public void whenCreatingRules_EnsureTypesAreCorrectlyConnectedToRulesOnCommit() throws InvalidKBException {
        try (Transaction tx = session.writeTransaction()) {
            Type p = tx.putEntityType("p");
            Type q = tx.putEntityType("q");
            Type r = tx.putEntityType("r");
            Type s = tx.putEntityType("s");
            Type t = tx.putEntityType("t");

            tx.putRule("Rule1",
                    Graql.parsePattern("{" +
                            "$x isa p;" +
                            "$x isa q;" +
                            "not{$x isa r;};" +
                            "};"),
                    Graql.parsePattern("$x isa s;"));

            tx.putRule("Rule2",
                    Graql.parsePattern("{" +
                            "$x isa q;" +
                            "$x isa r;" +
                            "not{$x isa s;};" +
                            "};"),
                    Graql.parsePattern("$x isa t;"));

            Sets.newHashSet(p, q, r, s, t).forEach(type -> {
                assertTrue(!type.whenRules().findFirst().isPresent());
                assertTrue(!type.thenRules().findFirst().isPresent());
            });

            tx.commit();
        }
        Transaction tx = session.readTransaction();
        Type p = tx.getEntityType("p");
        Type q = tx.getEntityType("q");
        Type r = tx.getEntityType("r");
        Type s = tx.getEntityType("s");
        Type t = tx.getEntityType("t");
        Rule rule = tx.getRule("Rule1");
        Rule rule2 = tx.getRule("Rule2");

        assertEquals(p.whenRules().collect(Collectors.toSet()), Sets.newHashSet(rule));
        assertEquals(q.whenRules().collect(Collectors.toSet()), Sets.newHashSet(rule, rule2));
        assertEquals(r.whenRules().collect(Collectors.toSet()), Sets.newHashSet(rule, rule2));
        assertEquals(s.whenRules().collect(Collectors.toSet()), Sets.newHashSet(rule2));
        assertThat(t.whenRules().collect(Collectors.toSet()), empty());

        Sets.newHashSet(p, q, r).forEach(type -> assertTrue(!type.thenRules().findFirst().isPresent()));
        assertEquals(s.thenRules().collect(Collectors.toSet()), Sets.newHashSet(rule));
        assertEquals(t.thenRules().collect(Collectors.toSet()), Sets.newHashSet(rule2));
        tx.close();
    }

    @Test
    public void whenAddingDuplicateRulesOfTheSameTypeWithTheSamePattern_ReturnTheSameRule() {
        try (Transaction tx = session.writeTransaction()) {
            tx.putEntityType("someType");
            Pattern when = Graql.parsePattern("$x isa someType;");
            Pattern then = Graql.parsePattern("$x isa someType;");

            Rule rule1 = tx.putRule("My-Angry-Rule", when, then);
            Rule rule2 = tx.putRule("My-Angry-Rule", when, then);

            assertEquals(rule1, rule2);
        }
    }

    @Ignore("This is ignored because we currently have no way to determine if patterns with different variables name are equivalent")
    @Test
    public void whenAddingDuplicateRulesOfTheSameTypeWithDifferentPatternVariables_ReturnTheSameRule() {
        try (Transaction tx = session.writeTransaction()) {
            tx.putEntityType("someType");
            Pattern when = Graql.parsePattern("$x isa someType;");
            Pattern then = Graql.parsePattern("$y isa someType;");

            Rule rule1 = tx.putRule("My-Angry-Rule", when, then);
            Rule rule2 = tx.putRule("My-Angry-Rule", when, then);

            assertEquals(rule1, rule2);
        }
    }

    @Test
    public void whenCreatingRuleWithImpossibleRole_rejectRule() {
        try (Transaction tx = session.writeTransaction()) {
            tx.execute(Graql.parse("define " +
                    "valid-rel sub relation, relates left, relates right;" +
                    "other-rel sub relation, relates unplayed;" +
                    "player sub entity, plays left, plays right;"
            ).asDefine());
            tx.execute(Graql.insert(Sets.newHashSet(
                    Graql.var("x").isa("player"),
                    Graql.var("r").rel("left", "x").rel("right", "x").isa("valid-rel"))
            ));
            tx.putRule("invalid-role-assignment",
                    Graql.var("r").isa("valid-rel").rel("left", "x").rel("right", "y"),
                    Graql.var().isa("other-rel").rel("unplayed", "x")
            );

            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage("Rule [invalid-role-assignment] asserts [$x] plays role [unplayed] that it can never play");
            tx.commit();
        }
    }

    public static <T> T[] concat(T[] first, T[] second) {
        T[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    private void validateOntologicallyIllegalRule(Pattern when, Pattern then, ErrorMessage message, String... outputParams) {
        try (Transaction tx = session.writeTransaction()) {
            initTx(tx);
            Rule rule = tx.putRule(UUID.randomUUID().toString(), when, then);

            String[] ruleParam = {rule.label().getValue()};
            Object[] params = concat(ruleParam, outputParams);
            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(message.getMessage(params));

            tx.commit();
        }
    }

    private void validateIllegalRule(Pattern when, Pattern then, ErrorMessage message) {
        try (Transaction tx = session.writeTransaction()) {
            initTx(tx);
            Rule rule = tx.putRule(UUID.randomUUID().toString(), when, then);

            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(message.getMessage(rule.label()));
            tx.commit();
        }
    }

    private void validateIllegalHead(Pattern when, Pattern then, ErrorMessage message) {
        try (Transaction tx = session.writeTransaction()) {
            initTx(tx);
            Rule rule = tx.putRule(UUID.randomUUID().toString(), when, then);

            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(message.getMessage(then.toString(), rule.label()));
            tx.commit();
        }
    }

    private void validateLegalHead(Pattern when, Pattern then) {
        try (Transaction tx = session.writeTransaction()) {
            initTx(tx);
            tx.putRule(UUID.randomUUID().toString(), when, then);
            tx.commit();
        }
    }

    private void initTx(Transaction tx) {
        AttributeType<Integer> someAttribute = tx.putAttributeType("someAttribute", AttributeType.DataType.INTEGER);
        AttributeType<Integer> anotherAttribute = tx.putAttributeType("anotherAttribute", AttributeType.DataType.INTEGER);
        AttributeType<String> stringAttribute = tx.putAttributeType("stringAttribute", AttributeType.DataType.STRING);
        Role someRole = tx.putRole("someRole");
        Role anotherRole = tx.putRole("anotherRole");
        Role singleRole = tx.putRole("singleRole");

        tx.putEntityType("someEntity")
                .has(someAttribute)
                .has(anotherAttribute)
                .has(stringAttribute)
                .plays(someRole)
                .plays(anotherRole);

        tx.putRelationType("someRelation")
                .relates(someRole)
                .relates(anotherRole)
                .relates(singleRole)
                .plays(someRole)
                .plays(anotherRole);
        tx.putRelationType("anotherRelation")
                .relates(singleRole);
    }
}