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

import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Rule;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.exception.InvalidKBException;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

public class RuleTest {
    @org.junit.Rule
    public final ExpectedException expectedException = ExpectedException.none();
    

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private Transaction tx;
    private Session session;

    @Before
    public void setUp(){
        session = server.sessionWithNewKeyspace();
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }
    
    @Test
    public void whenCreatingRulesWithNullValues_Throw() throws Exception {
        expectedException.expect(NullPointerException.class);
        tx.putRule("A Thing", null, null);
    }

    @Test
    public void whenCreatingRulesWithNonExistentEntityType_Throw() throws InvalidKBException {
        tx.putEntityType("My-Type");
        
        Pattern when = Graql.parsePattern("$x isa Your-Type");
        Pattern then = Graql.parsePattern("$x isa My-Type");
        Rule rule = tx.putRule("My-Sad-Rule-Type", when, then);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_RULE_MISSING_ELEMENTS.getMessage(Schema.VertexProperty.RULE_WHEN.name(), rule.label(), "Your-Type"));

        tx.commit();
    }

    @Test
    public void whenCreatingDistinctRulesWithSimilarStringHashes_EnsureRulesDoNotClash(){
        String productRefused = "productRefused";
        Pattern when1 = Graql.parsePattern("{$step has step-id 9; $e (process-case: $case) isa process-record; $case has consent false;}");
        Pattern then1 = Graql.parsePattern("{(record: $e, step: $step) isa record-step;}");
        Rule rule1 = tx.putRule(productRefused, when1, then1);

        String productAccepted = "productAccepted";
        Pattern when2 = Graql.parsePattern("{$step has step-id 7; $e (process-case: $case) isa process-record; $case has consent true;}");
        Pattern then2 = Graql.parsePattern("{(record: $e, step: $step) isa record-step;}");
        Rule rule2 = tx.putRule(productAccepted, when2, then2);

        assertEquals(rule1, tx.getRule(productRefused));
        assertEquals(rule2, tx.getRule(productAccepted));
    }

    @Test
    public void whenAddingRuleWithDisjunctionInTheBody_Throw() throws InvalidKBException {
        validateIllegalRule(
                Graql.parsePattern("(role: $x) or (role: $x, role: $y)"),
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_BODY
        );
    }

    @Test
    public void whenAddingRuleWithDisjunctionInTheHead_Throw() throws InvalidKBException {
        validateIllegalRule(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("(role1: $x) or (role1: $x, role2: $y)"),
                ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleWithNonAtomicHead_Throw() throws InvalidKBException {
        validateIllegalRule(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("{(role1: $x, role2: $y) isa relation1; $x has res1 'value';}"),
                ErrorMessage.VALIDATION_RULE_HEAD_NON_ATOMIC
        );
        validateIllegalRule(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("{role1: $x, role2: $y) isa relation1; (role1: $y, role2: $z) isa relation1;}"),
                ErrorMessage.VALIDATION_RULE_HEAD_NON_ATOMIC
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithUnboundRolePlayer_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("(role1: $y, role2: $z) isa relation1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithUnboundRelationVariable_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("{" +
                        "$r1 (role1: $x, role2: $y) isa relation1;" +
                        "$r2 (role1: $z, role2: $u) isa relation1;" +
                        "}"),
                Graql.parsePattern("$r (role1: $r1, role2: $r2) isa relation1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithUnboundVariablePredicate_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("$x has res1 $r"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_IsaAtomWithoutType_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("{$x isa $z;}"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_AMBIGUOUS_SCHEMA_CONCEPT
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithInequality_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("$x has res1 >10"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RESOURCE_WITH_NONSPECIFIC_PREDICATE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithBoundVariablePredicate_DoNotThrow(){
        validateLegalHead(
                Graql.parsePattern("$x has res1 $r"),
                Graql.parsePattern("$x has res2 $r")
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithAmbiguousPredicates_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("{$x has res1 $r; $r == 10; $r == 20;}"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RESOURCE_WITH_AMBIGUOUS_PREDICATES
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithMetaRoles_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("(role: $y, role: $x) isa relation1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithMissingRoles_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("($x, $y) isa relation1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithVariableRoles_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("($r1: $x, $r2: $y) isa relation1"),
                Graql.parsePattern("($r2: $x, $r1: $y) isa relation1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithoutType_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("(role3: $y, role3: $x)"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_AMBIGUOUS_SCHEMA_CONCEPT
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithImplicitType_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("($x, $y) isa " + Schema.ImplicitType.HAS.getLabel("res1").getValue()),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_IMPLICIT_SCHEMA_CONCEPT
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithImplicitRole_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("(" + Schema.ImplicitType.HAS_OWNER.getLabel("res1").getValue() + ": $x, $y)"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_IMPLICIT_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_IllegalTypeAtoms_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("$x sub entity1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("$x plays role1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("$x isa relation1"),
                Graql.parsePattern("$x relates role1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("$x has res1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_Predicate_Throw() throws InvalidKBException {
        ConceptId randomId = tx.getMetaConcept().id();
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("$x id '" + randomId.getValue() + "'"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("$x != $y'"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("($x, $y); $x isa res1;"),
                Graql.parsePattern("$x == '100'"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("$x != $y'"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalRule(
                Graql.parsePattern("($x, $y); $x isa res1;"),
                Graql.parsePattern("$x == '100'"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalRule(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("$x label 'entity1'"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_PropertyAtoms_Throw() throws InvalidKBException {
        validateIllegalHead(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("$x is-abstract"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("$x has res1 $y"),
                Graql.parsePattern("$y datatype string"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                Graql.parsePattern("$x isa entity1"),
                Graql.parsePattern("$x regex /entity/"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_RelationDoesntRelateARole_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(role1: $x, role3: $y) isa relation2"),
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED.getMessage("role1", "relation2")
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("(role1: $x, role3: $y) isa relation2"),
                ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED.getMessage("role1", "relation2")
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_TypeCantPlayARole_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("{$y isa entity1; }"),
                Graql.parsePattern("(role3: $x, role3: $y) isa relation2"),
                ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE.getMessage("entity1", "role3", "relation2")
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("{$y isa entity1; (role3: $x, role3: $y) isa relation2;}"),
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE.getMessage("entity1", "role3", "relation2")
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("{$y isa entity1; (role3: $x, role3: $y) isa relation2;}"),
                ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE.getMessage("entity1", "role3", "relation2")
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_EntityCantHaveResource_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("$x isa relation1"),
                Graql.parsePattern("$x has res1 'value'"),
                ErrorMessage.VALIDATION_RULE_ATTRIBUTE_OWNER_CANNOT_HAVE_ATTRIBUTE.getMessage("res1", "relation1")
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("{$x isa relation1, has res1 'value';}"),
                Graql.parsePattern("$x isa relation2"),
                ErrorMessage.VALIDATION_RULE_ATTRIBUTE_OWNER_CANNOT_HAVE_ATTRIBUTE.getMessage("res1", "relation1")
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("$x isa relation2"),
                Graql.parsePattern("{$x isa relation1, has res1 'value';"),
                ErrorMessage.VALIDATION_RULE_ATTRIBUTE_OWNER_CANNOT_HAVE_ATTRIBUTE.getMessage("res1", "relation1")
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_RelationWithInvalidType_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(role1: $x, role3: $y) isa res1"),
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE.getMessage("res1")
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("(role1: $x, role3: $y) isa res1"),
                ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE.getMessage("res1")
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_ResourceWithInvalidType_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("$x has relation1 'value'"),
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                ErrorMessage.VALIDATION_RULE_INVALID_ATTRIBUTE_TYPE.getMessage("relation1")
        );
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("$x has relation1 'value'"),
                ErrorMessage.VALIDATION_RULE_INVALID_ATTRIBUTE_TYPE.getMessage("relation1")
        );
    }

    @Test
    public void whenAddingRuleWithOntologicallyInvalidHead_RelationDoesntRelateARole_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                Graql.parsePattern("(role1: $x, role2: $y) isa relation1"),
                Graql.parsePattern("(role1: $x, role3: $y) isa relation2"),
                ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED.getMessage("role1", "relation2")
        );
    }

    private void validateOntologicallyIllegalRule(Pattern when, Pattern then, String message){
        initTx(tx);
        tx.putRule(UUID.randomUUID().toString(), when, then);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(message);

        tx.commit();
    }
    
    private void validateIllegalRule(Pattern when, Pattern then, ErrorMessage message){
        initTx(tx);
        Rule rule = tx.putRule(UUID.randomUUID().toString(), when, then);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(message.getMessage(rule.label()));
        tx.commit();
    }

    private void validateIllegalHead(Pattern when, Pattern then, ErrorMessage message){
        initTx(tx);
        Rule rule = tx.putRule(UUID.randomUUID().toString(), when, then);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(message.getMessage(then.toString(), rule.label()));
        tx.commit();
    }

    private void validateLegalHead(Pattern when, Pattern then){
        initTx(tx);
        tx.putRule(UUID.randomUUID().toString(), when, then);
        tx.commit();
    }
    
    private void initTx(Transaction tx){
        AttributeType<Integer> res1 = tx.putAttributeType("res1", AttributeType.DataType.INTEGER);
        AttributeType<Integer> res2 = tx.putAttributeType("res2", AttributeType.DataType.INTEGER);
        Role role1 = tx.putRole("role1");
        Role role2 = tx.putRole("role2");
        Role role3 = tx.putRole("role3");

        tx.putEntityType("entity1")
                .has(res1)
                .has(res2)
                .plays(role1)
                .plays(role2);

        tx.putRelationshipType("relation1")
                .relates(role1)
                .relates(role2)
                .relates(role3)
                .plays(role1)
                .plays(role2);
        tx.putRelationshipType("relation2")
                .relates(role3);
    }

    @Test
    public void whenCreatingRules_EnsureHypothesisAndConclusionTypesAreFilledOnCommit() throws InvalidKBException {
        EntityType t1 = tx.putEntityType("type1");
        EntityType t2 = tx.putEntityType("type2");

        Pattern when = Graql.parsePattern("$x isa type1");
        Pattern then = Graql.parsePattern("$x isa type2");

        Rule rule = tx.putRule("My-Happy-Rule", when, then);
        assertThat(rule.whenTypes().collect(Collectors.toSet()), empty());
        assertThat(rule.thenTypes().collect(Collectors.toSet()), empty());

        tx.commit();

        assertThat(rule.whenTypes().collect(Collectors.toSet()), containsInAnyOrder(t1));
        assertThat(rule.thenTypes().collect(Collectors.toSet()), containsInAnyOrder(t2));
    }

    @Test
    public void whenAddingDuplicateRulesOfTheSameTypeWithTheSamePattern_ReturnTheSameRule(){
        tx.putEntityType("type1");
        Pattern when = Graql.parsePattern("$x isa type1");
        Pattern then = Graql.parsePattern("$x isa type1");

        Rule rule1 = tx.putRule("My-Angry-Rule", when, then);
        Rule rule2 = tx.putRule("My-Angry-Rule", when, then);

        assertEquals(rule1, rule2);
    }

    @Ignore("This is ignored because we currently have no way to determine if patterns with different variables name are equivalent")
    @Test
    public void whenAddingDuplicateRulesOfTheSameTypeWithDifferentPatternVariables_ReturnTheSameRule(){
        tx.putEntityType("type1");
        Pattern when = Graql.parsePattern("$x isa type1");
        Pattern then = Graql.parsePattern("$y isa type1");

        Rule rule1 = tx.putRule("My-Angry-Rule", when, then);
        Rule rule2 = tx.putRule("My-Angry-Rule", when, then);

        assertEquals(rule1, rule2);
    }
}