/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.logic.resolvable;

import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import graql.lang.Graql;
import org.junit.Test;

import java.util.Set;

import static grakn.common.collection.Collections.list;
import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class ConcludableTest {

    private Conjunction parseConjunction(String query) {
        return Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    private long relationConcludablesCount(Set<Concludable> concludables) {
        return concludables.stream().filter(Concludable::isRelation).count();
    }

    private long hasConcludablesCount(Set<Concludable> concludables) {
        return concludables.stream().filter(Concludable::isHas).count();
    }

    private long isaConcludablesCount(Set<Concludable> concludables) {
        return concludables.stream().filter(Concludable::isIsa).count();
    }

    private long attributeConcludablesCount(Set<Concludable> concludables) {
        return concludables.stream().filter(Concludable::isAttribute).count();
    }

    @Test
    public void test_conjunction_only_isa_is_built() {
        String conjunction = "{ $a isa b; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_has_is_built() {
        String conjunction = "{ $a has $b; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_has_is_built_when_type_and_value_given() {
        String conjunction = "{ $a has age 50; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_relation_is_built() {
        String conjunction = "{ $a($x, $y); }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(1, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_relation_is_built_when_type_is_given() {
        String conjunction = "{ $a($x, $y) isa siblingship; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(1, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_has_and_relation_are_built_from_same_owner() {
        String conjunction = "{ $a($x, $y) isa siblingship, has number 2; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(1, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_isa_is_built_when_isa_owner_has_value() {
        String conjunction = "{ $a 5; $a isa age; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_value_is_built() {
        String conjunction = "{ $a 5; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(1, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_multiple_has_and_relation_are_built() {
        String conjunction = "{ $a($x, $y, $z) isa transaction, has currency \"GBP\", has value 45; " +
                "$b(locates: $a); $b has $c; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(3, hasConcludablesCount(concludables));
        assertEquals(2, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_multiple_relations_are_built() {
        String conjunction = "{ $a($x); $a($y); }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(2, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_only_creates_has_concludable_when_attribute_has_value_constraints() {
        String conjunction = "{ $x has $a; $a isa age; $a > 5; $a <= 10; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_creates_multiple_value_constraints_for_same_owned_attribute_without_an_isa() {
        String conjunction = "{ $x has $a; $a > 5; $a <= 10; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_creates_multiple_value_constraints_for_same_attribute() {
        String conjunction = "{ $a > 5; $a <= 10; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(1, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_creates_one_has_one_isa_concludable_for_attribute_owning_attribute() {
        String conjunction = "{ $a has $b; $a \"Hi there\" isa content; $b \"English\" isa language; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(1, isaConcludablesCount(concludables));
        assertEquals(1, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(0, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_creates_two_attribute_concludables_for_variable_value_comparison() {
        String conjunction = "{ $x > $y; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        assertEquals(0, isaConcludablesCount(concludables));
        assertEquals(0, hasConcludablesCount(concludables));
        assertEquals(0, relationConcludablesCount(concludables));
        assertEquals(2, attributeConcludablesCount(concludables));
    }

    @Test
    public void test_conjunction_concludables_does_not_contain_type_labels() {
        String conjunction = "{ $diag(patient-role-type: $per) isa $diagnosis; $diagnosis type diagnosis-type;" +
                "$per isa $person; $person type person-type; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        RelationConstraint relationConstraint = concludables.stream().filter(Concludable::isRelation).findFirst().get()
                .asRelation().relation();
        IsaConstraint relationIsaConstraint = relationConstraint.owner().isa().get();
        assertEquals("$diagnosis", relationIsaConstraint.type().reference().asName().syntax());
        assertFalse(relationIsaConstraint.type().label().isPresent());
        assertFalse(list(relationConstraint.players()).get(0).player().isa().isPresent());
    }

    @Test
    public void test_conjunction_concludables_contain_type_labels_with_anonymous_type_variable() {
        String conjunction = "{ $diag(patient-role-type: $per) isa diagnosis; $per isa person; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        RelationConstraint relationConstraint = concludables.stream().filter(Concludable::isRelation).findFirst()
                .get().asRelation().relation();
        IsaConstraint relationIsaConstraint = relationConstraint.owner().isa().get();
        assertEquals("$_diagnosis", relationIsaConstraint.type().reference().asLabel().syntax());
        assertEquals("diagnosis", relationIsaConstraint.type().label().get().label());
        assertFalse(list(relationConstraint.players()).get(0).player().isa().isPresent());
    }

    @Test
    public void test_conjunction_concludables_contain_type_labels_with_anonymous_type_and_relation_variable() {
        String conjunction = "{ (patient-role-type: $per) isa diagnosis; $per isa person; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        RelationConstraint relationConstraint = concludables.stream().filter(Concludable::isRelation).findFirst().get()
                .asRelation().relation();
        IsaConstraint relationIsaConstraint = relationConstraint.owner().isa().get();
        assertEquals("$_diagnosis", relationIsaConstraint.type().reference().asLabel().syntax());
        assertEquals("diagnosis", relationIsaConstraint.type().label().get().label());
        assertFalse(list(relationConstraint.players()).get(0).player().isa().isPresent());
    }

    @Test
    public void test_conjunction_anonymous_variable_constraints_are_not_conflated() {
        String conjunction = "{ (patient-role-type: $per) isa diagnosis; (patient-role-type: $p2) isa diagnosis; " +
                "$per isa person; $per has age 50; $per has age 70; }";
        Set<Concludable> concludables = Concludable.create(parseConjunction(conjunction));
        concludables.stream().filter(Concludable::isRelation).map(Concludable::asRelation).forEach(relationConcludable -> {
            assertTrue(relationConcludable.relation().owner().isa().isPresent());
        });
        concludables.stream().filter(Concludable::isHas).map(Concludable::asHas).forEach(hasConcludable -> {
            assertEquals(1, hasConcludable.has().attribute().value().size());
            assertTrue(hasConcludable.has().attribute().isa().isPresent());
        });
    }
}
