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
import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Rule;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.reasoner.rule.InferenceRule;
import grakn.core.graql.internal.reasoner.rule.RuleUtils;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
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

public class RuleTest {
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
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            expectedException.expect(NullPointerException.class);
            tx.putRule("A Thing", null, null);
        }
    }

    @Test
    public void whenCreatingRulesWithNonExistentEntityType_Throw() throws InvalidKBException {
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
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
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
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
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_BODY
        );
    }

    @Test
    public void whenAddingRuleWithDisjunctionInTheHead_Throw() throws InvalidKBException {
        validateIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("{(someRole: $x);} or {(someRole: $x, anotherRole: $y);};"),
                ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleWithNonAtomicHead_Throw() throws InvalidKBException {
        validateIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("{(someRole: $x, anotherRole: $y) isa relation; $x has res1 'value';};"),
                ErrorMessage.VALIDATION_RULE_HEAD_NON_ATOMIC
        );
        validateIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("{someRole: $x, anotherRole: $y) isa relation; (someRole: $y, anotherRole: $z) isa relation;};"),
                ErrorMessage.VALIDATION_RULE_HEAD_NON_ATOMIC
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithUnboundRolePlayer_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("(someRole: $y, anotherRole: $z) isa relation;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithUnboundRelationVariable_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("{" +
                        "$r1 (someRole: $x, anotherRole: $y) isa relation;" +
                        "$r2 (someRole: $z, anotherRole: $u) isa relation;" +
                        "};"),
                Graql.parsePattern("$r (someRole: $r1, anotherRole: $r2) isa relation;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithUnboundVariablePredicate_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("$x has res1 $r;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_IsaAtomWithoutType_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("{$x isa $z;};"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_AMBIGUOUS_SCHEMA_CONCEPT
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithInequality_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
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
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("{$x has res1 $r; $r == 10; $r == 20;};"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RESOURCE_WITH_AMBIGUOUS_PREDICATES
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithMetaRoles_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("(role: $y, role: $x) isa relation;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithMissingRoles_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("($x, $y) isa relation;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithVariableRoles_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("($r1: $x, $r2: $y) isa relation;"),
                Graql.parsePattern("($r2: $x, $r1: $y) isa relation;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithoutType_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("(singleRole: $y, singleRole: $x);"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_AMBIGUOUS_SCHEMA_CONCEPT
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithImplicitType_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("($x, $y) isa " + Schema.ImplicitType.HAS.getLabel("res1;").getValue()),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_IMPLICIT_SCHEMA_CONCEPT
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithImplicitRole_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("(" + Schema.ImplicitType.HAS_OWNER.getLabel("res1").getValue() + ": $x, $y);"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_IMPLICIT_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_IllegalTypeAtoms_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("$x sub someEntity;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("$x plays someRole;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("$x isa relation;"),
                Graql.parsePattern("$x relates someRole;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("$x has res1;"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_Predicate_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("$x id 'V123';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("$x != $y';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("($x, $y); $x isa res1;"),
                Graql.parsePattern("$x == '100';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("$x != $y';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalRule(
                Graql.parsePattern("($x, $y); $x isa res1;"),
                Graql.parsePattern("$x == '100';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("$x label 'someEntity';"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_PropertyAtoms_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
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
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED.getMessage("someRole", "anotherRelation")
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("(someRole: $x, singleRole: $y) isa anotherRelation;"),
                ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED.getMessage("someRole", "anotherRelation")
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_TypeCantPlayARole_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("{$x isa someEntity;$y isa someEntity; };"),
                Graql.parsePattern("(singleRole: $x, singleRole: $y) isa anotherRelation;"),
                ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE.getMessage("someEntity", "singleRole", "anotherRelation")
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("{$y isa someEntity; (singleRole: $x, singleRole: $y) isa anotherRelation;};"),
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE.getMessage("someEntity", "singleRole", "anotherRelation")
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("{(someRole: $x, anotherRole: $y) isa relation;$y isa someEntity;};"),
                Graql.parsePattern("{(singleRole: $x, singleRole: $y) isa anotherRelation;};"),
                ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE.getMessage("someEntity", "singleRole", "anotherRelation")
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_EntityCantHaveResource_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("$x isa relation;"),
                Graql.parsePattern("$x has res1 'value';"),
                ErrorMessage.VALIDATION_RULE_ATTRIBUTE_OWNER_CANNOT_HAVE_ATTRIBUTE.getMessage("res1", "relation")
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("{$x isa relation, has res1 'value';};"),
                Graql.parsePattern("$x isa anotherRelation;"),
                ErrorMessage.VALIDATION_RULE_ATTRIBUTE_OWNER_CANNOT_HAVE_ATTRIBUTE.getMessage("res1", "relation")
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("$x isa anotherRelation;"),
                Graql.parsePattern("{$x isa relation, has res1 'value';"),
                ErrorMessage.VALIDATION_RULE_ATTRIBUTE_OWNER_CANNOT_HAVE_ATTRIBUTE.getMessage("res1", "relation")
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_RelationWithInvalidType_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(someRole: $x, singleRole: $y) isa res1;"),
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE.getMessage("res1")
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("(someRole: $x, singleRole: $y) isa res1;"),
                ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE.getMessage("res1")
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_ResourceWithInvalidType_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("$x has relation 'value';"),
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                ErrorMessage.VALIDATION_RULE_INVALID_ATTRIBUTE_TYPE.getMessage("relation")
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("$x has relation 'value';"),
                ErrorMessage.VALIDATION_RULE_INVALID_ATTRIBUTE_TYPE.getMessage("relation")
        );
    }

    @Test
    public void whenAddingRuleWithOntologicallyInvalidHead_RelationDoesntRelateARole_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(someRole: $x, anotherRole: $y) isa relation;"),
                Graql.parsePattern("(someRole: $x, singleRole: $y) isa anotherRelation;"),
                ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED.getMessage("someRole", "anotherRelation")
        );
    }

    @Test
    public void whenAddingASimpleRuleWithNegationCycle_Throw() throws InvalidKBException{
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            Pattern when = Graql.parsePattern(
                "{" +
                        "$x isa entity;" +
                        "not {$y isa someEntity;};" +
                        "($x, $y) isa relation;" +
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
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
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
    public void whenAddingAddingStratifiableRules_correctStratificationIsProduced(){
        List<Rule> expected1;
        List<Rule> expected2;
        try(TransactionOLTP tx = session.transaction(Transaction.Type.WRITE)) {
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
        try(TransactionOLTP tx = session.transaction(Transaction.Type.WRITE)) {
            List<Rule> rules = RuleUtils.stratifyRules(tx.ruleCache().getRules().map(rule -> new InferenceRule(rule, tx)).collect(Collectors.toSet()))
                    .map(InferenceRule::getRule).collect(Collectors.toList());
            assertTrue(rules.equals(expected1) || rules.equals(expected2));
            expected1.forEach(Concept::delete);
            tx.commit();
        }
    }

    @Test
    public void whenAddingARuleWithMultipleNegationBlocks_Throw() throws InvalidKBException {
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            Pattern when = Graql.parsePattern(
                    "{" +
                            "$x isa thing;" +
                            "not {$x isa relation;};" +
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
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            Pattern when = Graql.parsePattern(
                    "{" +
                            "$x isa entity;" +
                            "not {" +
                                "$y isa relation;" +
                                "not {$y isa attribute;};" +
                            "};" +
                            "($x, $y) isa relation;" +
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
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            Pattern when = Graql.parsePattern(
                    "{" +
                            "$x isa entity;" +
                            "($x, $y) isa relation;" +
                            "not {" +
                                "{$y isa relation;} or " +
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
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
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
    public void whenAddingDuplicateRulesOfTheSameTypeWithTheSamePattern_ReturnTheSameRule(){
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
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
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType("someType");
            Pattern when = Graql.parsePattern("$x isa someType;");
            Pattern then = Graql.parsePattern("$y isa someType;");

            Rule rule1 = tx.putRule("My-Angry-Rule", when, then);
            Rule rule2 = tx.putRule("My-Angry-Rule", when, then);

            assertEquals(rule1, rule2);
        }
    }

    private void validateOntologicallyIllegalRule(Pattern when, Pattern then, String message){
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            initTx(tx);
            tx.putRule(UUID.randomUUID().toString(), when, then);

            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(message);

            tx.commit();
        }
    }

    private void validateIllegalRule(Pattern when, Pattern then, ErrorMessage message){
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            initTx(tx);
            Rule rule = tx.putRule(UUID.randomUUID().toString(), when, then);

            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(message.getMessage(rule.label()));
            tx.commit();
        }
    }

    private void validateIllegalHead(Pattern when, Pattern then, ErrorMessage message){
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            initTx(tx);
            Rule rule = tx.putRule(UUID.randomUUID().toString(), when, then);

            expectedException.expect(InvalidKBException.class);
            expectedException.expectMessage(message.getMessage(then.toString(), rule.label()));
            tx.commit();
        }
    }

    private void validateLegalHead(Pattern when, Pattern then){
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            initTx(tx);
            tx.putRule(UUID.randomUUID().toString(), when, then);
            tx.commit();
        }
    }

    private void initTx(Transaction tx){
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

        tx.putRelationType("relation")
                .relates(someRole)
                .relates(anotherRole)
                .relates(singleRole)
                .plays(someRole)
                .plays(anotherRole);
        tx.putRelationType("anotherRelation")
                .relates(singleRole);
    }
}