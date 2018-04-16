/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.kb.internal;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.Pattern;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;

//NOTE: This test is inside the graql module due to the inability to have graql constructs inside the graph module
public class RuleTest {
    @org.junit.Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private Pattern when;
    private Pattern then;
    private GraknTx graknTx;
    private GraknSession session;

    @Before
    public void setupRules(){
        session = Grakn.session(Grakn.IN_MEMORY, "absd");
        graknTx = Grakn.session(Grakn.IN_MEMORY, "absd").open(GraknTxType.WRITE);
    }

    @After
    public void closeSession() throws Exception {
        graknTx.close();
        session.close();
    }

    @Test
    public void whenCreatingRulesWithNullValues_Throw() throws Exception {
        expectedException.expect(NullPointerException.class);
        graknTx.putRule("A Thing", null, null);
    }

    @Test
    public void whenCreatingRulesWithNonExistentEntityType_Throw() throws InvalidKBException {
        graknTx.putEntityType("My-Type");

        when = graknTx.graql().parser().parsePattern("$x isa Your-Type");
        then = graknTx.graql().parser().parsePattern("$x isa My-Type");
        Rule rule = graknTx.putRule("My-Sad-Rule-Type", when, then);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_RULE_MISSING_ELEMENTS.getMessage(Schema.VertexProperty.RULE_WHEN.name(), rule.getLabel(), "Your-Type"));

        graknTx.commit();
    }

    @Test
    public void whenCreatingDistinctRulesWithSimilarStringHashes_EnsureRulesDoNotClash(){
        String productRefused = "productRefused";
        Pattern when1 = graknTx.graql().parser().parsePattern("{$step has step-id 9; $e (process-case: $case) isa process-record; $case has consent false;}");
        Pattern then1 = graknTx.graql().parser().parsePattern("{(record: $e, step: $step) isa record-step;}");
        Rule rule1 = graknTx.putRule(productRefused, when1, then1);

        String productAccepted = "productAccepted";
        Pattern when2 = graknTx.graql().parser().parsePattern("{$step has step-id 7; $e (process-case: $case) isa process-record; $case has consent true;}");
        Pattern then2 = graknTx.graql().parser().parsePattern("{(record: $e, step: $step) isa record-step;}");
        Rule rule2 = graknTx.putRule(productAccepted, when2, then2);

        assertEquals(rule1, graknTx.getRule(productRefused));
        assertEquals(rule2, graknTx.getRule(productAccepted));
    }

    @Test
    public void whenAddingRuleWithDisjunctionInTheBody_Throw() throws InvalidKBException {
        validateIllegalRule(
                graknTx.graql().parser().parsePattern("(role: $x) or (role: $x, role: $y)"),
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_BODY
        );
    }

    @Test
    public void whenAddingRuleWithDisjunctionInTheHead_Throw() throws InvalidKBException {
        validateIllegalRule(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("(role1: $x) or (role1: $x, role2: $y)"),
                ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleWithNonAtomicHead_Throw() throws InvalidKBException {
        validateIllegalRule(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("{(role1: $x, role2: $y) isa relation1; $x has res1 'value';}"),
                ErrorMessage.VALIDATION_RULE_HEAD_NON_ATOMIC
        );
        validateIllegalRule(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("{role1: $x, role2: $y) isa relation1; (role1: $y, role2: $z) isa relation1;}"),
                ErrorMessage.VALIDATION_RULE_HEAD_NON_ATOMIC
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithUnboundRolePlayer_Throw() throws InvalidKBException {
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("(role1: $y, role2: $z) isa relation1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithUnboundRelationVariable_Throw() throws InvalidKBException {
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("{" +
                        "$r1 (role1: $x, role2: $y) isa relation1;" +
                        "$r2 (role1: $z, role2: $u) isa relation1;" +
                        "}"),
                graknTx.graql().parser().parsePattern("$r (role1: $r1, role2: $r2) isa relation1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithUnboundVariablePredicate_Throw() throws InvalidKBException {
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("$x has res1 $r"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_UNBOUND_VARIABLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_IsaAtomWithoutType_Throw() throws InvalidKBException {
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("{$x isa $z;}"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_AMBIGUOUS_SCHEMA_CONCEPT
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithInequality_Throw() throws InvalidKBException {
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("$x has res1 >10"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RESOURCE_WITH_NONSPECIFIC_PREDICATE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithBoundVariablePredicate_DoNotThrow(){
        validateLegalHead(
                graknTx.graql().parser().parsePattern("$x has res1 $r"),
                graknTx.graql().parser().parsePattern("$x has res2 $r")
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_ResourceWithAmbiguousPredicates_Throw() throws InvalidKBException {
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("{$x has res1 $r; $r val =10; $r val =20;}"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RESOURCE_WITH_AMBIGUOUS_PREDICATES
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithMetaRoles_Throw() throws InvalidKBException {
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("(role: $y, role: $x) isa relation1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithMissingRoles_Throw() throws InvalidKBException {
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("($x, $y) isa relation1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithVariableRoles_Throw() throws InvalidKBException {
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("($r1: $x, $r2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("($r2: $x, $r1: $y) isa relation1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_AMBIGUOUS_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithoutType_Throw() throws InvalidKBException {
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("(role3: $y, role3: $x)"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_AMBIGUOUS_SCHEMA_CONCEPT
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithImplicitType_Throw() throws InvalidKBException {
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("($x, $y) isa " + Schema.ImplicitType.HAS.getLabel("res1").getValue()),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_ATOM_WITH_IMPLICIT_SCHEMA_CONCEPT
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_RelationWithImplicitRole_Throw() throws InvalidKBException {
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("(" + Schema.ImplicitType.HAS_OWNER.getLabel("res1").getValue() + ": $x, $y)"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_HEAD_RELATION_WITH_IMPLICIT_ROLE
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_IllegalTypeAtoms_Throw() throws InvalidKBException {
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("$x sub entity1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("$x plays role1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("$x isa relation1"),
                graknTx.graql().parser().parsePattern("$x relates role1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("$x has res1"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_Predicate_Throw() throws InvalidKBException {
        ConceptId randomId = graknTx.admin().getMetaConcept().getId();
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("$x id '" + randomId.getValue() + "'"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("$x != $y'"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("($x, $y); $x isa res1;"),
                graknTx.graql().parser().parsePattern("$x val '100'"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("$x != $y'"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalRule(
                graknTx.graql().parser().parsePattern("($x, $y); $x isa res1;"),
                graknTx.graql().parser().parsePattern("$x val '100'"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalRule(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("$x label 'entity1'"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleWithIllegalAtomicInHead_PropertyAtoms_Throw() throws InvalidKBException {
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("$x is-abstract"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("$x has res1 $y"),
                graknTx.graql().parser().parsePattern("$y datatype string"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
        validateIllegalHead(
                graknTx.graql().parser().parsePattern("$x isa entity1"),
                graknTx.graql().parser().parsePattern("$x regex /entity/"),
                ErrorMessage.VALIDATION_RULE_ILLEGAL_ATOMIC_IN_HEAD
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_RelationDoesntRelateARole_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                graknTx.graql().parser().parsePattern("(role1: $x, role3: $y) isa relation2"),
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED.getMessage("role1", "relation2")
        );
        validateOntologicallyIllegalRule(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("(role1: $x, role3: $y) isa relation2"),
                ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED.getMessage("role1", "relation2")
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_TypeCantPlayARole_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                graknTx.graql().parser().parsePattern("{$y isa entity1; }"),
                graknTx.graql().parser().parsePattern("(role3: $x, role3: $y) isa relation2"),
                ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE.getMessage("entity1", "role3", "relation2")
        );
        validateOntologicallyIllegalRule(
                graknTx.graql().parser().parsePattern("{$y isa entity1; (role3: $x, role3: $y) isa relation2;}"),
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE.getMessage("entity1", "role3", "relation2")
        );
        validateOntologicallyIllegalRule(
                graknTx.graql().parser().parsePattern("role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("{$y isa entity1; (role3: $x, role3: $y) isa relation2;}"),
                ErrorMessage.VALIDATION_RULE_TYPE_CANNOT_PLAY_ROLE.getMessage("entity1", "role3", "relation2")
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_EntityCantHaveResource_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                graknTx.graql().parser().parsePattern("$x isa relation1"),
                graknTx.graql().parser().parsePattern("$x has res1 'value'"),
                ErrorMessage.VALIDATION_RULE_RESOURCE_OWNER_CANNOT_HAVE_RESOURCE.getMessage("res1", "relation1")
        );
        validateOntologicallyIllegalRule(
                graknTx.graql().parser().parsePattern("{$x isa relation1, has res1 'value';}"),
                graknTx.graql().parser().parsePattern("$x isa relation2"),
                ErrorMessage.VALIDATION_RULE_RESOURCE_OWNER_CANNOT_HAVE_RESOURCE.getMessage("res1", "relation1")
        );
        validateOntologicallyIllegalRule(
                graknTx.graql().parser().parsePattern("$x isa relation2"),
                graknTx.graql().parser().parsePattern("{$x isa relation1, has res1 'value';"),
                ErrorMessage.VALIDATION_RULE_RESOURCE_OWNER_CANNOT_HAVE_RESOURCE.getMessage("res1", "relation1")
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_RelationWithInvalidType_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                graknTx.graql().parser().parsePattern("(role1: $x, role3: $y) isa res1"),
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE.getMessage("res1")
        );
        validateOntologicallyIllegalRule(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("(role1: $x, role3: $y) isa res1"),
                ErrorMessage.VALIDATION_RULE_INVALID_RELATION_TYPE.getMessage("res1")
        );
    }

    @Test
    public void whenAddingRuleInvalidOntologically_ResourceWithInvalidType_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                graknTx.graql().parser().parsePattern("$x has relation1 'value'"),
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                ErrorMessage.VALIDATION_RULE_INVALID_RESOURCE_TYPE.getMessage("relation1")
        );
        validateOntologicallyIllegalRule(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("$x has relation1 'value'"),
                ErrorMessage.VALIDATION_RULE_INVALID_RESOURCE_TYPE.getMessage("relation1")
        );
    }

    @Test
    public void whenAddingRuleWithOntologicallyInvalidHead_RelationDoesntRelateARole_Throw() throws InvalidKBException {
        validateOntologicallyIllegalRule(
                graknTx.graql().parser().parsePattern("(role1: $x, role2: $y) isa relation1"),
                graknTx.graql().parser().parsePattern("(role1: $x, role3: $y) isa relation2"),
                ErrorMessage.VALIDATION_RULE_ROLE_CANNOT_BE_PLAYED.getMessage("role1", "relation2")
        );
    }

    private void validateOntologicallyIllegalRule(Pattern when, Pattern then, String message){
        initTx(graknTx);
        graknTx.putRule(UUID.randomUUID().toString(), when, then);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(message);

        graknTx.commit();
    }
    
    private void validateIllegalRule(Pattern when, Pattern then, ErrorMessage message){
        initTx(graknTx);
        Rule rule = graknTx.putRule(UUID.randomUUID().toString(), when, then);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(message.getMessage(rule.getLabel()));
        graknTx.commit();
    }

    private void validateIllegalHead(Pattern when, Pattern then, ErrorMessage message){
        initTx(graknTx);
        Rule rule = graknTx.putRule(UUID.randomUUID().toString(), when, then);

        expectedException.expect(InvalidKBException.class);
        expectedException.expectMessage(message.getMessage(then.toString(), rule.getLabel()));
        graknTx.commit();
    }

    private void validateLegalHead(Pattern when, Pattern then){
        initTx(graknTx);
        graknTx.putRule(UUID.randomUUID().toString(), when, then);
        graknTx.commit();
    }
    
    private void initTx(GraknTx graph){
        AttributeType<Integer> res1 = graph.putAttributeType("res1", AttributeType.DataType.INTEGER);
        AttributeType<Integer> res2 = graph.putAttributeType("res2", AttributeType.DataType.INTEGER);
        Role role1 = graph.putRole("role1");
        Role role2 = graph.putRole("role2");
        Role role3 = graph.putRole("role3");

        graph.putEntityType("entity1")
                .attribute(res1)
                .attribute(res2)
                .plays(role1)
                .plays(role2);

        graph.putRelationshipType("relation1")
                .relates(role1)
                .relates(role2)
                .relates(role3)
                .plays(role1)
                .plays(role2);
        graph.putRelationshipType("relation2")
                .relates(role3);
    }

    @Test
    public void whenCreatingRules_EnsureHypothesisAndConclusionTypesAreFilledOnCommit() throws InvalidKBException {
        EntityType t1 = graknTx.putEntityType("type1");
        EntityType t2 = graknTx.putEntityType("type2");

        when = graknTx.graql().parser().parsePattern("$x isa type1");
        then = graknTx.graql().parser().parsePattern("$x isa type2");

        Rule rule = graknTx.putRule("My-Happy-Rule", when, then);
        assertThat(rule.getHypothesisTypes().collect(Collectors.toSet()), empty());
        assertThat(rule.getConclusionTypes().collect(Collectors.toSet()), empty());

        graknTx.commit();

        assertThat(rule.getHypothesisTypes().collect(Collectors.toSet()), containsInAnyOrder(t1));
        assertThat(rule.getConclusionTypes().collect(Collectors.toSet()), containsInAnyOrder(t2));
    }

    @Test
    public void whenAddingDuplicateRulesOfTheSameTypeWithTheSamePattern_ReturnTheSameRule(){
        graknTx.putEntityType("type1");
        when = graknTx.graql().parser().parsePattern("$x isa type1");
        then = graknTx.graql().parser().parsePattern("$x isa type1");

        Rule rule1 = graknTx.putRule("My-Angry-Rule", when, then);
        Rule rule2 = graknTx.putRule("My-Angry-Rule", when, then);

        assertEquals(rule1, rule2);
    }

    @Ignore("This is ignored because we currently have no way to determine if patterns with different variables name are equivalent")
    @Test
    public void whenAddingDuplicateRulesOfTheSameTypeWithDifferentPatternVariables_ReturnTheSameRule(){
        graknTx.putEntityType("type1");
        when = graknTx.graql().parser().parsePattern("$x isa type1");
        then = graknTx.graql().parser().parsePattern("$y isa type1");

        Rule rule1 = graknTx.putRule("My-Angry-Rule", when, then);
        Rule rule2 = graknTx.putRule("My-Angry-Rule", when, then);

        assertEquals(rule1, rule2);
    }
}
