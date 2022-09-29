/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.logic.resolvable;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.variable.Reference;
import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class UnifierTest {

    private Conjunction parseConjunction(String query) {
        return Disjunction.create(TypeQL.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    @Test
    public void test_constants_which_satisfy_value_constraint() {
        Pair<String, String>[] constraintValuePairs = new Pair[]{
                // Boolean
                new Pair("{ $x true; }", "{$y true;}"),
                new Pair("{ $x false; }", "{$y false;}"),
                new Pair("{ $x = true; }", "{$y true;}"),
                new Pair("{ $x = false; }", "{$y false;}"),
                new Pair("{ $x != true; }", "{$y false;}"),
                new Pair("{ $x != false; }", "{$y true;}"),

                // Numeric assign, equality, gte, lte
                new Pair("{ $x 1; }", "{$y 1;}"),
                new Pair("{ $x 1; }", "{$y 1.0;}"),
                new Pair("{ $x 1.0; }", "{$y 1;}"),
                new Pair("{ $x 1.0; }", "{$y 1.0;}"),

                new Pair("{ $x = 1; }", "{$y 1;}"),
                new Pair("{ $x = 1; }", "{$y 1.0;}"),
                new Pair("{ $x = 1.0; }", "{$y 1;}"),
                new Pair("{ $x = 1.0; }", "{$y 1.0;}"),

                new Pair("{ $x >= 1; }", "{$y 1;}"),
                new Pair("{ $x >= 1; }", "{$y 1.0;}"),
                new Pair("{ $x >= 1.0; }", "{$y 1;}"),
                new Pair("{ $x >= 1.0; }", "{$y 1.0;}"),

                new Pair("{ $x <= 1; }", "{$y 1;}"),
                new Pair("{ $x <= 1; }", "{$y 1.0;}"),
                new Pair("{ $x <= 1.0; }", "{$y 1;}"),
                new Pair("{ $x <= 1.0; }", "{$y 1.0;}"),

                // Numeric inequality, gt, lt
                new Pair("{ $x != 1; }", "{$y 2;}"),
                new Pair("{ $x != 1; }", "{$y 2.0;}"),
                new Pair("{ $x != 1.0; }", "{$y 2;}"),
                new Pair("{ $x != 1.0; }", "{$y 2.0;}"),

                new Pair("{ $x > 1; }", "{$y 2;}"),
                new Pair("{ $x > 1; }", "{$y 2.0;}"),
                new Pair("{ $x > 1.0; }", "{$y 2;}"),
                new Pair("{ $x > 1.0; }", "{$y 2.0;}"),

                new Pair("{ $x < 1; }", "{$y -2;}"),
                new Pair("{ $x < 1; }", "{$y -2.0;}"),
                new Pair("{ $x < 1.0; }", "{$y -2;}"),
                new Pair("{ $x < 1.0; }", "{$y -2.0;}"),

                // String comparisons
                new Pair("{ $x \"one\"; }", "{$y \"one\";}"),
                new Pair("{ $x = \"one\"; }", "{$y \"one\";}"),
                new Pair("{ $x >= \"one\"; }", "{$y \"one\";}"),
                new Pair("{ $x <= \"one\"; }", "{$y \"one\";}"),
                new Pair("{ $x != \"one\"; }", "{$y \"two\";}"),
                new Pair("{ $x > \"one\"; }", "{$y \"two\";}"),
                new Pair("{ $x < \"two\"; }", "{$y \"one\";}"),

                // String comparisons, case-sensitivity
                new Pair("{ $x != \"one\"; }", "{$y \"ONE\";}"),
                new Pair("{ $x > \"TWO\"; }", "{$y \"one\";}"),
                new Pair("{ $x < \"one\"; }", "{$y \"TWO\";}"),

                // DateTime
                new Pair("{ $x 2022-01-01; }", "{$y 2022-01-01;}"),
                new Pair("{ $x = 2022-01-01; }", "{$y  2022-01-01;}"),
                new Pair("{ $x != 2022-01-01; }", "{$y  2022-02-02;}"),

                new Pair("{ $x >= 2022-01-01; }", "{$y 2022-01-01;}"),
                new Pair("{ $x >= 2022-01-01; }", "{$y 2022-02-02;}"),
                new Pair("{ $x <= 2022-01-01; }", "{$y 2022-01-01;}"),
                new Pair("{ $x <= 2022-02-02; }", "{$y 2022-01-01;}"),

                new Pair("{ $x > 2022-01-01; }", "{$y 2022-02-02;}"),
                new Pair("{ $x < 2022-02-02; }", "{$y 2022-01-01;}"),
        };

        for (Pair<String, String> pair : constraintValuePairs) {
            Conjunction constraint = parseConjunction(pair.first());
            Conjunction value = parseConjunction(pair.second());

            assert (constraint.variable(Identifier.Variable.of(Reference.Name.name("x"))).asThing().value().size() > 0);
            assert (value.variable(Identifier.Variable.of(Reference.Name.name("y"))).asThing().value().size() > 0);
            assertTrue(Unifier.Builder.unificationSatisfiable(
                    constraint.variable(Identifier.Variable.of(Reference.Name.name("x"))).asThing(),
                    value.variable(Identifier.Variable.of(Reference.Name.name("y"))).asThing()
            ));
        }
    }

    @Test
    public void test_constants_which_dont_satisfy_value_constraint() {
        Pair<String, String>[] constraintValuePairs = new Pair[]{
                // Boolean
                new Pair("{ $x true; }", "{$y false;}"),
                new Pair("{ $x false; }", "{$y true;}"),
                new Pair("{ $x = true; }", "{$y false;}"),
                new Pair("{ $x = false; }", "{$y true;}"),
                new Pair("{ $x != true; }", "{$y true;}"),
                new Pair("{ $x != false; }", "{$y false;}"),

                // Numeric assign, equality, gte, lte
                new Pair("{ $x 1; }", "{$y 2;}"),
                new Pair("{ $x 1; }", "{$y 2.0;}"),
                new Pair("{ $x 1.0; }", "{$y 2;}"),
                new Pair("{ $x 1.0; }", "{$y 2.0;}"),

                new Pair("{ $x = 1; }", "{$y 2;}"),
                new Pair("{ $x = 1; }", "{$y 2.0;}"),
                new Pair("{ $x = 1.0; }", "{$y 2;}"),
                new Pair("{ $x = 1.0; }", "{$y 2.0;}"),

                new Pair("{ $x = 1; }", "{$y -1;}"),
                new Pair("{ $x = 1; }", "{$y -1.0;}"),
                new Pair("{ $x = 1.0; }", "{$y -1;}"),
                new Pair("{ $x = 1.0; }", "{$y -1.0;}"),

                new Pair("{ $x >= 1; }", "{$y -1;}"),
                new Pair("{ $x >= 1; }", "{$y -1.0;}"),
                new Pair("{ $x >= 1.0; }", "{$y -1;}"),
                new Pair("{ $x >= 1.0; }", "{$y -1.0;}"),

                new Pair("{ $x <= 1; }", "{$y 2;}"),
                new Pair("{ $x <= 1; }", "{$y 2.0;}"),
                new Pair("{ $x <= 1.0; }", "{$y 2;}"),
                new Pair("{ $x <= 1.0; }", "{$y 2.0;}"),

                // Numeric inequality, gt, lt
                new Pair("{ $x != 1; }", "{$y 1;}"),
                new Pair("{ $x != 1; }", "{$y 1.0;}"),
                new Pair("{ $x != 1.0; }", "{$y 1;}"),
                new Pair("{ $x != 1.0; }", "{$y 1.0;}"),

                new Pair("{ $x > 1; }", "{$y -1;}"),
                new Pair("{ $x > 1; }", "{$y -1.0;}"),
                new Pair("{ $x > 1.0; }", "{$y -1;}"),
                new Pair("{ $x > 1.0; }", "{$y -1.0;}"),

                new Pair("{ $x > 1; }", "{$y 1;}"),
                new Pair("{ $x > 1; }", "{$y 1.0;}"),
                new Pair("{ $x > 1.0; }", "{$y 1;}"),
                new Pair("{ $x > 1.0; }", "{$y 1.0;}"),

                new Pair("{ $x < 1; }", "{$y 1;}"),
                new Pair("{ $x < 1; }", "{$y 1.0;}"),
                new Pair("{ $x < 1.0; }", "{$y 1;}"),
                new Pair("{ $x < 1.0; }", "{$y 1.0;}"),

                new Pair("{ $x < 1; }", "{$y 2;}"),
                new Pair("{ $x < 1; }", "{$y 2.0;}"),
                new Pair("{ $x < 1.0; }", "{$y 2;}"),
                new Pair("{ $x < 1.0; }", "{$y 2.0;}"),

                // String comparisons
                new Pair("{ $x \"one\"; }", "{$y \"two\";}"),
                new Pair("{ $x = \"one\"; }", "{$y \"two\";}"),
                new Pair("{ $x >= \"two\"; }", "{$y \"one\";}"),
                new Pair("{ $x <= \"one\"; }", "{$y \"two\";}"),
                new Pair("{ $x != \"one\"; }", "{$y \"one\";}"),
                new Pair("{ $x > \"one\"; }", "{$y \"one\";}"),
                new Pair("{ $x > \"two\"; }", "{$y \"one\";}"),
                new Pair("{ $x < \"two\"; }", "{$y \"two\";}"),
                new Pair("{ $x < \"one\"; }", "{$y \"two\";}"),

                // String comparisons, case-sensitivity
                new Pair("{ $x \"OnE\"; }", "{$y \"oNe\";}"),
                new Pair("{ $x = \"OnE\"; }", "{$y \"oNe\";}"),
                new Pair("{ $x > \"two\"; }", "{$y \"ONE\";}"),
                new Pair("{ $x < \"ONE\"; }", "{$y \"two\";}"),

                // DateTime
                new Pair("{ $x 2022-01-01; }", "{$y  2022-02-02;}"),
                new Pair("{ $x = 2022-01-01; }", "{$y  2022-02-02;}"),
                new Pair("{ $x != 2022-01-01; }", "{$y  2022-01-01;}"),

                new Pair("{ $x >= 2022-02-02; }", "{$y 2022-01-01;}"),
                new Pair("{ $x <= 2022-01-01; }", "{$y 2022-02-02;}"),

                new Pair("{ $x > 2022-01-01; }", "{$y 2022-01-01;}"),
                new Pair("{ $x > 2022-02-02; }", "{$y 2022-01-01;}"),
                new Pair("{ $x < 2022-01-01; }", "{$y 2022-01-01;}"),
                new Pair("{ $x < 2022-01-01; }", "{$y 2022-02-02;}"),
        };

        for (Pair<String, String> pair : constraintValuePairs) {
            Conjunction constraint = parseConjunction(pair.first());
            Conjunction value = parseConjunction(pair.second());

            assert (constraint.variable(Identifier.Variable.of(Reference.Name.name("x"))).asThing().value().size() > 0);
            assert (value.variable(Identifier.Variable.of(Reference.Name.name("y"))).asThing().value().size() > 0);
            assertFalse(Unifier.Builder.unificationSatisfiable(
                    constraint.variable(Identifier.Variable.of(Reference.Name.name("x"))).asThing(),
                    value.variable(Identifier.Variable.of(Reference.Name.name("y"))).asThing()
            ));
        }
    }

    @Test
    public void test_constants_which_satisfy_substring_constraint() {
        Pair<String, String>[] constraintValuePairs = new Pair[]{
                new Pair("{ $x like \"[0-9]{2}-[a-z]{3}-[0-9]{4}\"; }", "{$y \"01-jan-2022\";}"),
                new Pair("{ $x contains \"jan\"; }", "{$y \"01-jan-2022\";}"),
        };

        for (Pair<String, String> pair : constraintValuePairs) {
            Conjunction constraint = parseConjunction(pair.first());
            Conjunction value = parseConjunction(pair.second());

            assert (constraint.variable(Identifier.Variable.of(Reference.Name.name("x"))).asThing().value().size() > 0);
            assert (value.variable(Identifier.Variable.of(Reference.Name.name("y"))).asThing().value().size() > 0);
            assertTrue(Unifier.Builder.unificationSatisfiable(
                    constraint.variable(Identifier.Variable.of(Reference.Name.name("x"))).asThing(),
                    value.variable(Identifier.Variable.of(Reference.Name.name("y"))).asThing()
            ));
        }
    }

    @Test
    public void test_constants_which_dont_satisfy_substring_constraint() {
        Pair<String, String>[] constraintValuePairs = new Pair[]{
                // Both constant
                new Pair("{ $x like \"[0-9]{2}-[a-z]{3}-[0-9]{4}\"; }", "{$y \"01-01-2022\";}"),
                new Pair("{ $x contains \"jan\"; }", "{$y \"01-feb-2022\";}"),
        };

        for (Pair<String, String> pair : constraintValuePairs) {
            Conjunction constraint = parseConjunction(pair.first());
            Conjunction value = parseConjunction(pair.second());

            assert (constraint.variable(Identifier.Variable.of(Reference.Name.name("x"))).asThing().value().size() > 0);
            assert (value.variable(Identifier.Variable.of(Reference.Name.name("y"))).asThing().value().size() > 0);
            assertFalse(Unifier.Builder.unificationSatisfiable(
                    constraint.variable(Identifier.Variable.of(Reference.Name.name("x"))).asThing(),
                    value.variable(Identifier.Variable.of(Reference.Name.name("y"))).asThing()
            ));
        }
    }

    @Test
    public void test_constraints_involving_a_variable_are_always_satisfied() {
        Pair<String, String>[] constraintValuePairs = new Pair[]{
                // Boolean
                new Pair("{ $x has attr $v; }", "{$y has attr true;}"),
                new Pair("{ $x has attr $v; }", "{$y has attr false;}"),

                new Pair("{ $x has attr = $v; }", "{$y has attr true;}"),
                new Pair("{ $x has attr = $v; }", "{$y has attr false;}"),

                new Pair("{ $x has attr != $v; }", "{$y has attr true;}"),
                new Pair("{ $x has attr != $v; }", "{$y has attr false;}"),

                // Numeric assign, equality, gte, lte
                new Pair("{ $x has attr $v; }", "{$y has attr 1;}"),
                new Pair("{ $x has attr $v; }", "{$y has attr 1.0;}"),

                new Pair("{ $x has attr = $v; }", "{$y has attr 1;}"),
                new Pair("{ $x has attr = $v; }", "{$y has attr 1.0;}"),

                new Pair("{ $x has attr >= $v; }", "{$y has attr 1;}"),
                new Pair("{ $x has attr >= $v; }", "{$y has attr 1.0;}"),

                new Pair("{ $x has attr <= $v; }", "{$y has attr 1;}"),
                new Pair("{ $x has attr <= $v; }", "{$y has attr 1.0;}"),

                // Numeric inequality, gt, lt
                new Pair("{ $x has attr != $v; }", "{$y has attr 1;}"),
                new Pair("{ $x has attr != $v; }", "{$y has attr 1.0;}"),

                new Pair("{ $x has attr > $v; }", "{$y has attr 1;}"),
                new Pair("{ $x has attr > $v; }", "{$y has attr 1.0;}"),

                new Pair("{ $x has attr < $v; }", "{$y has attr 1;}"),
                new Pair("{ $x has attr < $v; }", "{$y has attr 1.0;}"),

                // String comparisons
                new Pair("{ $x has attr = $v; }", "{$y has attr \"one\";}"),
                new Pair("{ $x has attr >= $v; }", "{$y has attr \"one\";}"),
                new Pair("{ $x has attr <= $v; }", "{$y has attr \"one\";}"),
                new Pair("{ $x has attr != $v; }", "{$y has attr \"one\";}"),
                new Pair("{ $x has attr > $v; }", "{$y has attr \"one\";}"),
                new Pair("{ $x has attr < $v; }", "{$y has attr \"one\";}"),

                // DateTime
                new Pair("{ $x has attr $v; }", "{$y has attr 2022-01-01;}"),
                new Pair("{ $x has attr = $v; }", "{$y has attr 2022-01-01;}"),
                new Pair("{ $x has attr != $v; }", "{$y has attr 2022-01-01;}"),
                new Pair("{ $x has attr >= $v; }", "{$y has attr 2022-01-01;}"),
                new Pair("{ $x has attr <= $v; }", "{$y has attr 2022-01-01;}"),
                new Pair("{ $x has attr > $v; }", "{$y has attr 2022-01-01;}"),
                new Pair("{ $x has attr < $v; }", "{$y has attr 2022-01-01;}"),
        };

        for (Pair<String, String> pair : constraintValuePairs) {
            Conjunction constraint = parseConjunction(pair.first());
            Conjunction value = parseConjunction(pair.second());

            ThingVariable constraintAttrVar = constraint.variable(Identifier.Variable.of(Reference.Name.name("x"))).asThing()
                    .constraints().stream().filter(c -> c.isHas()).map(c -> c.asHas().attribute()).findAny().get();

            ThingVariable valueAttrVar = value.variable(Identifier.Variable.of(Reference.Name.name("y"))).asThing()
                    .constraints().stream().filter(c -> c.isHas()).map(c -> c.asHas().attribute()).findAny().get();

            assert (constraintAttrVar.value().size() > 0 || valueAttrVar.value().size() > 0);
            assertTrue(Unifier.Builder.unificationSatisfiable(constraintAttrVar, valueAttrVar));
        }
    }

    @Test
    public void test_variables_always_satisfy_value_constraint() {
        Pair<String, String>[] constraintValuePairs = new Pair[]{
                // Boolean
                new Pair("{ $x has attr true; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr false; }", "{$y has attr $v;}"),

                new Pair("{ $x has attr = true; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr = false; }", "{$y has attr $v;}"),

                new Pair("{ $x has attr != true; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr != false; }", "{$y has attr $v;}"),

                // Numeric assign, equality, gte, lte
                new Pair("{ $x has attr 1; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr 1.0; }", "{$y has attr $v;}"),

                new Pair("{ $x has attr = 1; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr = 1.0; }", "{$y has attr $v;}"),

                new Pair("{ $x has attr >= 1; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr >= 1.0; }", "{$y has attr $v;}"),

                new Pair("{ $x has attr <= 1; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr <= 1.0; }", "{$y has attr $v;}"),

                // Numeric inequality, gt, lt
                new Pair("{ $x has attr != 1; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr != 1.0; }", "{$y has attr $v;}"),

                new Pair("{ $x has attr > 1; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr > 1.0; }", "{$y has attr $v;}"),

                new Pair("{ $x has attr < 1; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr < 1.0; }", "{$y has attr $v;}"),

                // String comparisons
                new Pair("{ $x has attr \"one\"; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr >= \"one\"; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr <= \"one\"; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr != \"one\"; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr > \"one\"; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr < \"one\"; }", "{$y has attr $v;}"),

                // DateTime
                new Pair("{ $x has attr 2022-01-01; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr = 2022-01-01; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr != 2022-01-01; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr >= 2022-01-01; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr <= 2022-01-01; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr > 2022-01-01; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr < 2022-01-01; }", "{$y has attr $v;}"),

                // Substring
                new Pair("{ $x has attr like \"\"; }", "{$y has attr $v;}"),
                new Pair("{ $x has attr contains \"jan\"; }", "{$y has attr $v;}"),
        };

        for (Pair<String, String> pair : constraintValuePairs) {
            Conjunction constraint = parseConjunction(pair.first());
            Conjunction value = parseConjunction(pair.second());

            ThingVariable constraintAttrVar = constraint.variable(Identifier.Variable.of(Reference.Name.name("x"))).asThing()
                    .constraints().stream().filter(c -> c.isHas()).map(c -> c.asHas().attribute()).findAny().get();
            ThingVariable valueAttrVar = value.variable(Identifier.Variable.of(Reference.Name.name("y"))).asThing()
                    .constraints().stream().filter(c -> c.isHas()).map(c -> c.asHas().attribute()).findAny().get();

            assert (constraintAttrVar.value().size() > 0 || valueAttrVar.value().size() > 0);
            assertTrue(Unifier.Builder.unificationSatisfiable(constraintAttrVar, valueAttrVar));
        }
    }

    @Test
    public void test_variables_always_satisfy_constraints_involving_a_variable() {
        Pair<String, String>[] constraintValuePairs = new Pair[]{
                // Both var
                new Pair("{ $x has attr $v; }", "{$y has attr $w;}"),
                new Pair("{ $x has attr = $v; }", "{$y has attr $w;}"),
                new Pair("{ $x has attr != $v; }", "{$y has attr $w;}"),
                new Pair("{ $x has attr <= $v; }", "{$y has attr $w;}"),
                new Pair("{ $x has attr >= $v; }", "{$y has attr $w;}"),
                new Pair("{ $x has attr < $v; }", "{$y has attr $w;}"),
                new Pair("{ $x has attr > $v; }", "{$y has attr $w;}"),
        };

        for (Pair<String, String> pair : constraintValuePairs) {
            Conjunction constraint = parseConjunction(pair.first());
            Conjunction value = parseConjunction(pair.second());

            ThingVariable constraintAttrVar = constraint.variable(Identifier.Variable.of(Reference.Name.name("x"))).asThing()
                    .constraints().stream().filter(c -> c.isHas()).map(c -> c.asHas().attribute()).findAny().get();
            ThingVariable valueAttrVar = value.variable(Identifier.Variable.of(Reference.Name.name("y"))).asThing()
                    .constraints().stream().filter(c -> c.isHas()).map(c -> c.asHas().attribute()).findAny().get();

            assertTrue(Unifier.Builder.unificationSatisfiable(constraintAttrVar, valueAttrVar));
        }
    }

}
