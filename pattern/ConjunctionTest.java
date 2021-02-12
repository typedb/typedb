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

package grakn.core.pattern;

import grakn.core.pattern.variable.Variable;
import graql.lang.Graql;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.set;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class ConjunctionTest {

    private Conjunction parse(String query) {
        return Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    private Set<String> conjunctionStringToStatementStrings(String conjunctionString) {
        assertTrue(conjunctionString.startsWith("{ "));
        assertTrue(conjunctionString.endsWith(" }"));
        String withoutNewlines = conjunctionString.replace("\n", "");
        return set(Arrays.asList(withoutNewlines.substring(2, withoutNewlines.length() - 1).split("; ")));
    }

    private int expectedNewlines(Set<String> expectedConstraintStrings) {
        Set<String> uniqueConstraintOwners = new HashSet<>();
        expectedConstraintStrings.forEach(s -> uniqueConstraintOwners.add(s.substring(0, s.indexOf(" "))));
        return uniqueConstraintOwners.size() - 1;
    }

    private int newlinesIn(String string) {
        return string.length() - string.replace("\n", "").length();
    }

    @Test
    public void test_conjunction_with_entity_attribute_and_relation() {
        Conjunction conjunction = parse(
                "{ $p isa person, has $n; $n \"Alice\" isa name; $e(employee: $p) isa employment; }");

        Set<String> variableStrings = conjunction.variables().stream().map(Variable::toString).collect(Collectors.toSet());
        Set<String> expectedVariableStrings = set(
                "$p", "$_person", "$n", "$_name", "$e", "$_employment", "$_employment:employee");
        assertEquals(expectedVariableStrings, variableStrings);

        Set<String> expectedConstraintStrings = set("$p isa $_person", "$_person type person", "$p has $n",
                                                    "$n isa $_name", "$_name type name", "$n = \"Alice\"",
                                                    "$e ($_employment:employee:$p)", "$e isa $_employment",
                                                    "$_employment type employment",
                                                    "$_employment:employee type employment:employee");
        String conjunctionString = conjunction.toString();
        assertEquals(expectedNewlines(expectedConstraintStrings), newlinesIn(conjunctionString));
        assertEquals(expectedConstraintStrings, conjunctionStringToStatementStrings(conjunctionString));
    }

    @Test
    public void test_conjunction_with_attribute_syntactic_sugar() {
        Conjunction conjunction = parse("{ $p isa person, has name $n; }");

        Set<String> variableStrings = conjunction.variables().stream().map(Variable::toString).collect(Collectors.toSet());
        Set<String> expectedVariableStrings = Stream.of("$n", "$p", "$_name", "$_person").collect(Collectors.toSet());
        assertEquals(expectedVariableStrings, variableStrings);

        Set<String> expectedConstraintStrings = set("$p isa $_person", "$_person type person", "$p has $n",
                                                    "$n isa $_name", "$_name type name");
        String conjunctionString = conjunction.toString();
        assertEquals(expectedNewlines(expectedConstraintStrings), newlinesIn(conjunctionString));
        assertEquals(expectedConstraintStrings, conjunctionStringToStatementStrings(conjunctionString));
    }

    @Test
    public void test_schema_conjunction() {
        Conjunction conjunction = parse("{ $p sub person, plays $r; $e sub employment, relates $r; }");

        Set<String> variableStrings = conjunction.variables().stream().map(Variable::toString).collect(Collectors.toSet());
        Set<String> expectedVariableStrings = set("$p", "$_person", "$r", "$e", "$_employment");
        assertEquals(expectedVariableStrings, variableStrings);

        Set<String> expectedConstraintStrings = set(
                "$p sub $_person", "$_person type person", "$p plays $r", "$e sub $_employment",
                "$_employment type employment", "$e relates $r");
        String conjunctionString = conjunction.toString();
        assertEquals(expectedNewlines(expectedConstraintStrings), newlinesIn(conjunctionString));
        assertEquals(expectedConstraintStrings, conjunctionStringToStatementStrings(conjunctionString));
    }

    @Test
    public void test_conjunction_with_iids() {
        Conjunction conjunction = parse("{ $x iid 0x12345678; $y iid 0x87654321; }");

        Set<String> variableStrings = conjunction.variables().stream().map(Variable::toString).collect(Collectors.toSet());
        Set<String> expectedVariableStrings = set("$x", "$y");
        assertEquals(expectedVariableStrings, variableStrings);

        Set<String> expectedConstraintStrings = set("$x iid 0x12345678", "$y iid 0x87654321");
        String conjunctionString = conjunction.toString();
        assertEquals(expectedNewlines(expectedConstraintStrings), newlinesIn(conjunctionString));
        assertEquals(expectedConstraintStrings, conjunctionStringToStatementStrings(conjunctionString));
    }
}
