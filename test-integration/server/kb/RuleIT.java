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

package grakn.core.server.kb;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.concept.Concept;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.Rule;
import grakn.core.concept.type.Type;
import grakn.core.graql.reasoner.rule.InferenceRule;
import grakn.core.graql.reasoner.rule.RuleUtils;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
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

public class RuleIT {
    @org.junit.Rule
    public final ExpectedException expectedException = ExpectedException.none();
    

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private SessionImpl session;

    @Before
    public void setUp(){
        session = server.sessionWithNewKeyspace();
    }

    @After
    public void tearDown(){
        session.close();
    }
    
    @Test
    public void whenCreatingRulesWithNullValues_Throw() throws NullPointerException {
        try(TransactionOLTP tx = session.transaction().write()) {
            expectedException.expect(NullPointerException.class);
            tx.putRule("A Thing", null, null);
        }
    }

    @Test
    public void whenCreatingRulesWithNonExistentEntityType_Throw() throws InvalidKBException {
        try(TransactionOLTP tx = session.transaction().write()) {
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
    public void whenCreatingDistinctRulesWithSimilarStringHashes_EnsureRulesDoNotClash(){
        try(TransactionOLTP tx = session.transaction().write()) {
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
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_BODY
        );
    }

    @Test
    public void whenAddingRuleWithDisjunctionInTheHead_Throw() throws InvalidKBException {
        validateIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("{(someRole: $x);} or {(someRole: $x, anotherRole: $y);};"),
                ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleWithNonAtomicHead_Throw() throws InvalidKBException {
        validateIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("{(someRole: $x, anotherRole: $y) isa some-relation; $x has res1 'value';};"),
                ErrorMessage.VALIDATION_RULE_HEAD_NON_ATOMIC
        );
        validateIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("{someRole: $x, anotherRole: $y) isa some-relation; (someRole: $y, anotherRole: $z) isa some-relation;};"),
                ErrorMessage.VALIDATION_RULE_HEAD_NON_ATOMIC
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithUnboundRolePlayer_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("(someRole: $y, anotherRole: $z) isa some-relation;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithUnboundRelationVariable_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("{" +
                        "$r1 (someRole: $x, anotherRole: $y) isa some-relation;" +
                        "$r2 (someRole: $z, anotherRole: $u) isa some-relation;" +
                        "};"),
                Graql.parsePattern("$r (someRole: $r1, anotherRole: $r2) isa some-relation;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithUnboundVariablePredicate_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("$x has res1 $r;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_IsaAtomWithoutType_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("{$x isa $z;};"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_AMBIGUOUS_SCHEMA_CONCEPT
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithInequality_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("$x has res1 >10;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RESOURCE_WITH_NONSPECIFIC_PREDICATE
        );
    }

    @Test
    public void whenAddingRuleWithLegalAtomicInHead_ResourceWithBoundVariablePredicate_DoNotThrow(){
        validateLegalHead(
                Graql.parsePattern("$x has res1 $r;"),
                Graql.parsePattern("$x has res2 $r;")
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithAmbiguousPredicates_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("{$x has res1 $r; $r == 10; $r == 20;};"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RESOURCE_WITH_AMBIGUOUS_PREDICATES
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithMetaRoles_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("(role: $y, role: $x) isa some-relation;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithMissingRoles_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("($x, $y) isa some-relation;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithVariableRoles_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("($r1: $x, $r2: $y) isa some-relation;"),
                Graql.parsePattern("($r2: $x, $r1: $y) isa some-relation;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithoutType_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("(singleRole: $y, singleRole: $x);"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_AMBIGUOUS_SCHEMA_CONCEPT
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithImplicitType_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("($x, $y) isa " + Schema.ImplicitType.HAS.getLabel("res1;").getValue()),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_IMPLICIT_SCHEMA_CONCEPT
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithImplicitRole_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("(" + Schema.ImplicitType.HAS_OWNER.getLabel("res1").getValue() + ": $x, $y);"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_IMPLICIT_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_IllegalTypeAtoms_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("$x sub someEntity;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("$x plays someRole;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("$x isa some-relation;"),
                Graql.parsePattern("$x relates someRole;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("$x has res1;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_Predicate_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("$x id 'V123';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("$x != $y';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("($x, $y); $x isa res1;"),
                Graql.parsePattern("$x == '100';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("$x != $y';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalRule(
                Graql.parsePattern("($x, $y); $x isa res1;"),
                Graql.parsePattern("$x == '100';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("$x label 'someEntity';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_PropertyAtoms_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("$x abstract;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("$x has res1 $y;"),
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
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED,
                "someRole", "anotherRelation"
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
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
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE,
                "someEntity", "singleRole", "anotherRelation"
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("{(someRole: $x, anotherRole: $y) isa some-relation;$y isa someEntity;};"),
                Graql.parsePattern("{(singleRole: $x, singleRole: $y) isa anotherRelation;};"),
                ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE,
                "someEntity", "singleRole", "anotherRelation"
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_EntityCantHaveResource_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("$x isa some-relation;"),
                Graql.parsePattern("$x has res1 'value';"),
                ErrorMessage.VALIDATION_RULE_ATTRIBUTE_OWNER_CANNOT_HAVE_ATTRIBUTE,
                "res1", "some-relation"
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("{$x isa some-relation, has res1 'value';};"),
                Graql.parsePattern("$x isa anotherRelation;"),
                ErrorMessage.VALIDATION_RULE_ATTRIBUTE_OWNER_CANNOT_HAVE_ATTRIBUTE,
                "res1", "some-relation"
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("$x isa anotherRelation;"),
                Graql.parsePattern("{$x isa some-relation, has res1 'value';"),
                ErrorMessage.VALIDATION_RULE_ATTRIBUTE_OWNER_CANNOT_HAVE_ATTRIBUTE,
                "res1", "some-relation"
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_RelationWithInvalidType_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(someRole: $x, singleRole: $y) isa res1;"),
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE,
                "res1"
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("(someRole: $x, singleRole: $y) isa res1;"),
                ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE,
                "res1"
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_ResourceWithInvalidType_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("$x has some-relation 'value';"),
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                ErrorMessage.VALIDATION_RULE_INVALID_ATTRIBUTE_TYPE,
                "some-relation"
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("$x has some-relation 'value';"),
                ErrorMessage.VALIDATION_RULE_INVALID_ATTRIBUTE_TYPE,
                "some-relation"
        );
    }

    @Test
    public void whenAddingRuleWithOntologicallyInvalidHead_RelationDoesntRelateARole_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa some-relation;"),
                Graql.parsePattern("(someRole: $x, singleRole: $y) isa anotherRelation;"),
                ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED,
                "someRole", "anotherRelation"
        );
    }

    @Test
    public void whenAddingASimpleRuleWithNegationCycle_Throw() throws InvalidKBException{
        try(TransactionOLTP tx = session.transaction().write()) {
            Pattern when = Graql.parsePattern(
                "{" +
                        "$x isa entity;" +
                        "not {$y isa someEntity;};" +
                        "($x, $y) isa some-relation;" +
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
    public void whenAddingNonStratifiableRules_Throw() throws InvalidKBException{
        try(TransactionOLTP tx = session.transaction().write()) {
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
    public void whenAdditionOfRuleWithMetaTypeMakesRulesNonstratifiable_Throw() throws InvalidKBException{
        try(TransactionOLTP tx = session.transaction().write()) {
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

        try(TransactionOLTP tx = session.transaction().write()) {
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
    public void whenAdditionOfRuleMakesRulesNonstratifiable_Throw() throws InvalidKBException{
        try(TransactionOLTP tx = session.transaction().write()) {
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

        try(TransactionOLTP tx = session.transaction().write()) {
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
    public void whenAddingAddingStratifiableRules_correctStratificationIsProduced(){
        List<Rule> expected1;
        List<Rule> expected2;
        try(TransactionOLTP tx = session.transaction().write()) {
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
            expected1 = Lists.newArrayList(Rs, Rr, Rp1, Rp2);
            expected2 = Lists.newArrayList(Rs, Rr, Rp2, Rp1);

            tx.commit();
        }
        try(TransactionOLTP tx = session.transaction().write()) {
            List<Rule> rules = RuleUtils.stratifyRules(tx.ruleCache().getRules().map(rule -> new InferenceRule(rule, tx)).collect(Collectors.toSet()))
                    .map(InferenceRule::getRule).collect(Collectors.toList());
            assertTrue(rules.equals(expected1) || rules.equals(expected2));
            expected1.forEach(Concept::delete);
            tx.commit();
        }
    }

    @Test
    public void whenAddingRulesWithSimpleContradiction_Throw() throws InvalidKBException {
        try (TransactionOLTP tx = session.transaction().write()) {
            EntityType p = tx.putEntityType("p");
            EntityType q = tx.putEntityType("q");

            tx.putRule("Rule",
                    Graql.parsePattern("{$x isa entity; not{ $x isa q;};};"),
                    Graql.parsePattern("$x isa p;"));
            tx.putRule("Rule2",
                    Graql.parsePattern("$x isa q;"),
                    Graql.parsePattern("$x isa p;"));

            expectedException.expect(InvalidKBException.class);

            expectedException.expectMessage(allOf(
                    containsString("The rule graph contains a contradiction"),
                    containsString(p.toString()),
                    containsString(q.toString())
            ));

            tx.commit();
        }
    }

    @Test
    public void whenAddingRuleWithContradictionWithinIt_Throw() throws InvalidKBException {
        try (TransactionOLTP tx = session.transaction().write()) {
            EntityType p = tx.putEntityType("p");
            EntityType q = tx.putEntityType("q");

            tx.putRule("Rule",
                    Graql.parsePattern("{$x isa entity; $x isa q;not{ $x isa q;};};"),
                    Graql.parsePattern("$x isa p;"));

            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(allOf(
                    containsString("The rule graph contains a contradiction"),
                    containsString(p.toString()),
                    containsString(q.toString())
            ));

            tx.commit();
        }
    }

    @Test
    public void whenAddingRulesWithNonTrivialContradiction_Throw() throws InvalidKBException {
        try (TransactionOLTP tx = session.transaction().write()) {
            EntityType p = tx.putEntityType("p");
            EntityType q = tx.putEntityType("q");
            EntityType r = tx.putEntityType("r");
            EntityType s = tx.putEntityType("s");
            EntityType t = tx.putEntityType("t");

            //negative path to r
            tx.putRule("Rule1",
                    Graql.parsePattern("{$x isa p; not{ $x isa q;};};"),
                    Graql.parsePattern("$x isa r;"));

            //positive path to r
            tx.putRule("Rule2",
                    Graql.parsePattern("{$x isa q;};"),
                    Graql.parsePattern("$x isa t;"));

            tx.putRule("Rule3",
                    Graql.parsePattern("{$x isa t;};"),
                    Graql.parsePattern("$x isa r;"));

            expectedException.expect(InvalidKBException.class);

            expectedException.expectMessage(allOf(
                    containsString("The rule graph contains a contradiction"),
                    containsString(q.toString()),
                    containsString(r.toString())
            ));

            tx.commit();
        }
    }

    @Test
    public void whenAddingARuleWithMultipleNegationBlocks_Throw() throws InvalidKBException {
        try (TransactionOLTP tx = session.transaction().write()) {
            Pattern when = Graql.parsePattern(
                    "{" +
                            "$x isa thing;" +
                            "not {$x isa some-relation;};" +
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
    public void whenAddingARuleWithNestedNegationBlock_Throw() throws InvalidKBException{
        try(TransactionOLTP tx = session.transaction().write()) {
            Pattern when = Graql.parsePattern(
                    "{" +
                            "$x isa entity;" +
                            "not {" +
                                "$y isa some-relation;" +
                                "not {$y isa attribute;};" +
                            "};" +
                            "($x, $y) isa some-relation;" +
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
    public void whenAddingARuleWithDisjunctiveNegationBlock_Throw() throws InvalidKBException{
        try(TransactionOLTP tx = session.transaction().write()) {
            Pattern when = Graql.parsePattern(
                    "{" +
                            "$x isa entity;" +
                            "($x, $y) isa some-relation;" +
                            "not {" +
                                "{$y isa some-relation;} or " +
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
        try(TransactionOLTP tx = session.transaction().write()) {
            EntityType t1 = tx.putEntityType("someType");
            EntityType t2 = tx.putEntityType("anotherType");

            Pattern when = Graql.parsePattern("$x isa someType;");
            Pattern then = Graql.parsePattern("$x isa anotherType;");

            Rule rule = tx.putRule("My-Happy-Rule", when, then);
            assertThat(rule.whenTypes().collect(Collectors.toSet()), empty());
            assertThat(rule.thenTypes().collect(Collectors.toSet()), empty());

            tx.commit();

            assertThat(rule.whenTypes().collect(Collectors.toSet()), containsInAnyOrder(t1));
            assertThat(rule.thenTypes().collect(Collectors.toSet()), containsInAnyOrder(t2));
        }
    }

    @Test
    public void whenCreatingRules_EnsureWhenTypesAndSignAreLinkedCorrectlyOnCommit() throws InvalidKBException {
        try(TransactionOLTP tx = session.transaction().write()) {
            Type p = tx.putEntityType("p");
            Type q = tx.putEntityType("q");
            Type r = tx.putEntityType("r");
            Type s = tx.putEntityType("s");
            Type t = tx.putEntityType("t");

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

            assertEquals(rule.whenTypes().collect(Collectors.toSet()), Sets.newHashSet(p, q, r, s));
            assertEquals(rule.whenPositiveTypes().collect(Collectors.toSet()), Sets.newHashSet(p, q));
            assertEquals(rule.whenNegativeTypes().collect(Collectors.toSet()), Sets.newHashSet(r, s));
            assertThat(rule.thenTypes().collect(Collectors.toSet()), containsInAnyOrder(t));
        }
    }

    @Test
    public void whenCreatingRules_EnsureTypesAreCorrectlyConnectedToRulesOnCommit() throws InvalidKBException {
        try(TransactionOLTP tx = session.transaction().write()) {
            Type p = tx.putEntityType("p");
            Type q = tx.putEntityType("q");
            Type r = tx.putEntityType("r");
            Type s = tx.putEntityType("s");
            Type t = tx.putEntityType("t");

            Rule rule = tx.putRule("Rule1",
                    Graql.parsePattern("{" +
                            "$x isa p;" +
                            "$x isa q;" +
                            "not{$x isa r;};" +
                            "};"),
                    Graql.parsePattern("$x isa s;"));

            Rule rule2 = tx.putRule("Rule2",
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

            assertEquals(p.whenRules().collect(Collectors.toSet()), Sets.newHashSet(rule));
            assertEquals(q.whenRules().collect(Collectors.toSet()), Sets.newHashSet(rule, rule2));
            assertEquals(r.whenRules().collect(Collectors.toSet()), Sets.newHashSet(rule, rule2));
            assertEquals(s.whenRules().collect(Collectors.toSet()), Sets.newHashSet(rule2));
            assertThat(t.whenRules().collect(Collectors.toSet()), empty());

            Sets.newHashSet(p, q, r).forEach(type -> assertTrue(!type.thenRules().findFirst().isPresent()));
            assertEquals(s.thenRules().collect(Collectors.toSet()), Sets.newHashSet(rule));
            assertEquals(t.thenRules().collect(Collectors.toSet()), Sets.newHashSet(rule2));
        }
    }

    @Test
    public void whenAddingDuplicateRulesOfTheSameTypeWithTheSamePattern_ReturnTheSameRule(){
        try(TransactionOLTP tx = session.transaction().write()) {
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
    public void whenAddingDuplicateRulesOfTheSameTypeWithDifferentPatternVariables_ReturnTheSameRule(){
        try(TransactionOLTP tx = session.transaction().write()) {
            tx.putEntityType("someType");
            Pattern when = Graql.parsePattern("$x isa someType;");
            Pattern then = Graql.parsePattern("$y isa someType;");

            Rule rule1 = tx.putRule("My-Angry-Rule", when, then);
            Rule rule2 = tx.putRule("My-Angry-Rule", when, then);

            assertEquals(rule1, rule2);
        }
    }

    public static <T> T[] concat(T[] first, T[] second) {
        T[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    private void validateOntologicallyIllegalRule(Pattern when, Pattern then, ErrorMessage message, String... outputParams){
        try(TransactionOLTP tx = session.transaction().write()) {
            initTx(tx);
            Rule rule = tx.putRule(UUID.randomUUID().toString(), when, then);

            String[] ruleParam = {rule.label().getValue()};
            Object[] params = concat(ruleParam, outputParams);
            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(message.getMessage(params));

            tx.commit();
        }
    }

    private void validateIllegalRule(Pattern when, Pattern then, ErrorMessage message){
        try(TransactionOLTP tx = session.transaction().write()) {
            initTx(tx);
            Rule rule = tx.putRule(UUID.randomUUID().toString(), when, then);

            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(message.getMessage(rule.label()));
            tx.commit();
        }
    }

    private void validateIllegalHead(Pattern when, Pattern then, ErrorMessage message){
        try(TransactionOLTP tx = session.transaction().write()) {
            initTx(tx);
            Rule rule = tx.putRule(UUID.randomUUID().toString(), when, then);

            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(message.getMessage(then.toString(), rule.label()));
            tx.commit();
        }
    }

    private void validateLegalHead(Pattern when, Pattern then){
        try(TransactionOLTP tx = session.transaction().write()) {
            initTx(tx);
            tx.putRule(UUID.randomUUID().toString(), when, then);
            tx.commit();
        }
    }

    private void initTx(TransactionOLTP tx){
        AttributeType<Integer> res1 = tx.putAttributeType("res1", AttributeType.DataType.INTEGER);
        AttributeType<Integer> res2 = tx.putAttributeType("res2", AttributeType.DataType.INTEGER);
        Role someRole = tx.putRole("someRole");
        Role anotherRole = tx.putRole("anotherRole");
        Role singleRole = tx.putRole("singleRole");

        tx.putEntityType("someEntity")
                .has(res1)
                .has(res2)
                .plays(someRole)
                .plays(anotherRole);

        tx.putRelationType("some-relation")
                .relates(someRole)
                .relates(anotherRole)
                .relates(singleRole)
                .plays(someRole)
                .plays(anotherRole);
        tx.putRelationType("anotherRelation")
                .relates(singleRole);
    }
}