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
 */

package grakn.core.logic.concludable;

import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.Variable;
import graql.lang.Graql;
import graql.lang.pattern.variable.Reference;
import org.junit.Test;

import java.util.Set;

import static grakn.common.collection.Collections.list;
import static grakn.core.pattern.variable.VariableRegistry.createFromThings;
import static graql.lang.common.GraqlToken.Predicate.Equality.GT;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class ConcludableTest {

    private Conjunction parseConjunction(String query) {
        return Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    private ThingVariable parseThingVariable(String graqlVariable, String variableName) {
        return createFromThings(list(Graql.parseVariable(graqlVariable).asThing())).get(Reference.named(variableName)).asThing();
    }

    private long relationConcludablesCount(Set<ConjunctionConcludable<?, ?>> concludables) {
        return concludables.stream().filter(ConjunctionConcludable::isRelation).count();
    }

    private long hasConcludablesCount(Set<ConjunctionConcludable<?, ?>> concludables) {
        return concludables.stream().filter(ConjunctionConcludable::isHas).count();
    }

    private long isaConcludablesCount(Set<ConjunctionConcludable<?, ?>> concludables) {
        return concludables.stream().filter(ConjunctionConcludable::isIsa).count();
    }

    private long valueConcludablesCount(Set<ConjunctionConcludable<?, ?>> concludables) {
        return concludables.stream().filter(ConjunctionConcludable::isValue).count();
    }

    @Test
    public void test_conjunction_only_isa_is_built() {
        String conjunction = "{ $a isa b; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, valueConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_has_is_built() {
        String conjunction = "{ $a has $b; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, valueConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_has_is_built_when_type_and_value_given() {
        String conjunction = "{ $a has age 50; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, valueConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_relation_is_built() {
        String conjunction = "{ $a($x, $y); }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(1, relationConcludablesCount(concludables));
        assertEquals(0, valueConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_relation_is_built_when_type_is_given() {
        String conjunction = "{ $a($x, $y) isa siblingship; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(1, relationConcludablesCount(concludables));
        assertEquals(0, valueConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_has_and_relation_are_built_from_same_owner() {
        String conjunction = "{ $a($x, $y) isa siblingship, has number 2; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(1, relationConcludablesCount(concludables));
        assertEquals(0, valueConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_isa_is_built_when_isa_owner_has_value() {
        String conjunction = "{ $a 5; $a isa age; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, valueConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_value_is_built() {
        String conjunction = "{ $a 5; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(1, valueConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_multiple_has_and_relation_are_built() {
        String conjunction = "{ $a($x, $y, $z) isa transaction, has currency \"GBP\", has value 45; " +
                "$b(locates: $a); $b has $c; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(3, hasConcludablesCount(concludables));
        assertEquals(2, relationConcludablesCount(concludables));
        assertEquals(0, valueConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_multiple_relations_are_built() {
        String conjunction = "{ $a($x); $a($y); }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(2, relationConcludablesCount(concludables));
        assertEquals(0, valueConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_creates_has_concludable_when_attribute_has_value_constraints() {
        String conjunction = "{ $x has $a; $a isa age; $a > 5; $a <= 10; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, valueConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_creates_multiple_value_constraints_for_same_owned_attribute_without_an_isa() {
        String conjunction = "{ $x has $a; $a > 5; $a <= 10; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(2, valueConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_creates_multiple_value_constraints_for_same_attribute() {
        String conjunction = "{ $a > 5; $a <= 10; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(2, valueConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_creates_one_has_one_isa_concludable_for_attribute_owning_attribute() {
        String conjunction = "{ $a has $b; $a \"Hi there\" isa content; $b \"English\" isa language; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, valueConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_concludables_contain_type_labels() {
        String conjunction = "{ $diag(patient-role-type: $per) isa $diagnosis; $diagnosis type diagnosis-type;" +
                "$per isa $person; $person type person-type; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        RelationConstraint relationConstraint = concludables.stream().filter(ConjunctionConcludable::isRelation).findFirst().get()
                .asRelation().constraint();
        IsaConstraint relationIsaConstraint = relationConstraint.owner().isa().get();
        assertEquals("$diagnosis", relationIsaConstraint.type().reference().asName().syntax());
        assertEquals("diagnosis-type", relationIsaConstraint.type().label().get().label());
        assertEquals("person-type", relationConstraint.players().get(0).player().isa().get().type().label()
                .get().label());
    }

    @Test
    public void test_conjunction_concludables_contain_type_labels_with_anonymous_type_variable() {
        String conjunction = "{ $diag(patient-role-type: $per) isa diagnosis; $per isa person; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        RelationConstraint relationConstraint = concludables.stream().filter(ConjunctionConcludable::isRelation).findFirst()
                .get().asRelation().constraint();
        IsaConstraint relationIsaConstraint = relationConstraint.owner().isa().get();
        assertEquals("$_diagnosis", relationIsaConstraint.type().reference().asLabel().syntax());
        assertEquals("diagnosis", relationIsaConstraint.type().label().get().label());
        assertEquals("person", relationConstraint.players().get(0).player().isa().get().type().label().get().label());
    }

    @Test
    public void test_conjunction_concludables_contain_type_labels_with_anonymous_type_and_relation_variable() {
        String conjunction = "{ (patient-role-type: $per) isa diagnosis; $per isa person; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        RelationConstraint relationConstraint = concludables.stream().filter(ConjunctionConcludable::isRelation).findFirst().get()
                .asRelation().constraint();
        IsaConstraint relationIsaConstraint = relationConstraint.owner().isa().get();
        assertEquals("$_diagnosis", relationIsaConstraint.type().reference().asLabel().syntax());
        assertEquals("diagnosis", relationIsaConstraint.type().label().get().label());
        assertEquals("person", relationConstraint.players().get(0).player().isa().get().type().label().get().label());
    }

    @Test
    public void test_conjunction_anonymous_variable_constraints_are_not_conflated() {
        String conjunction = "{ (patient-role-type: $per) isa diagnosis; (patient-role-type: $p2) isa diagnosis; " +
                "$per isa person; $per has age 50; $per has age 70; }";
        Set<ConjunctionConcludable<?, ?>> concludables = ConjunctionConcludable.create(parseConjunction(conjunction));
        concludables.stream().filter(ConjunctionConcludable::isRelation).map(ConjunctionConcludable::asRelation).forEach(relationConcludable -> {
            assertEquals(0, relationConcludable.constraint().owner().value().size());
            assertTrue(relationConcludable.constraint().owner().isa().isPresent());
        });
        concludables.stream().filter(ConjunctionConcludable::isHas).map(ConjunctionConcludable::asHas).forEach(hasConcludable -> {
            assertEquals(1, hasConcludable.constraint().attribute().value().size());
            assertTrue(hasConcludable.constraint().owner().isa().isPresent());
        });
    }

    // --------------- Head concludables ---------------

    @Test
    public void test_isa_owner_has_value() {
        Conjunction conjunction = parseConjunction("{ $a 5; $a isa age; }");
        ThingVariable variable = parseThingVariable("$a isa age", "a");
        IsaConstraint isaConstraint = variable.isa().get();
        ThenConcludable.Isa isaConcludable = new ThenConcludable.Isa(isaConstraint, conjunction.variables());
        assertEquals(5, isaConcludable.constraint().owner().value().iterator().next().asLong().value().longValue());
    }

    @Test
    public void test_isa_owner_has_value_predicate() {
        Conjunction conjunction = parseConjunction("{ $a > 5; $a isa age; }");
        ThingVariable variable = parseThingVariable("$a isa age", "a");
        IsaConstraint isaConstraint = variable.isa().get();
        ThenConcludable.Isa isaConcludable = new ThenConcludable.Isa(isaConstraint, conjunction.variables());
        assertEquals(5, isaConcludable.constraint().owner().value().iterator().next().asLong().value().longValue());
        assertEquals(GT, isaConcludable.constraint().owner().value().iterator().next().asLong().predicate().asEquality());
    }

    @Test
    public void test_has_concludable_gathers_owner_type_label() {
        ThingVariable variable = parseThingVariable("$x has $a", "x");
        HasConstraint hasConstraint = variable.has().iterator().next();
        Set<Variable> contextVariables = parseConjunction("{ $x isa $p; $p type person; }").variables();
        ThenConcludable.Has hasConcludable = new ThenConcludable.Has(hasConstraint, contextVariables);
        assertEquals("person", hasConcludable.constraint().owner().isa().get().type().label().get().label());
    }

    @Test
    public void test_type_label_constraint_passed_to_concludable() {
        ThingVariable variable = parseThingVariable("$diag(patient: $per) isa $diagnosis", "diag");
        RelationConstraint relationConstraint = variable.relation().iterator().next();
        Set<Variable> contextVariables = parseConjunction("{ $per isa $person; $person type person-type; }").variables();
        ThenConcludable.Relation relationConcludable = new ThenConcludable.Relation(relationConstraint, contextVariables);
        assertEquals("person-type", relationConcludable.constraint().players().get(0).player().isa().get()
                .type().label().get().label());
    }
}
