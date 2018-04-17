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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.template;

import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Query;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ai.grakn.graql.Graql.parse;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;

public class TemplateParserTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void templateReplacesOneValuedOnOneLine_ValuesReplacedCorrectly(){
        String template = "insert $x isa person has name <name>;    ";
        String expected = "insert $x0 has name \"Phil Collins\" isa person;";

        Map<String, Object> data = new HashMap<>();
        data.put("name", "Phil Collins");

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateReplacesMultipleValuesOnOneLine_ValuesReplacedCorrectly(){
        String template = "insert $x isa person has name <name> , has feet <numFeet>;";
        String expected = "insert $x0 has name \"Phil Collins\" isa person has feet 3;";

        Map<String, Object> data = new HashMap<>();
        data.put("name", "Phil Collins");
        data.put("numFeet", 3);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateExecutedWithMissingData_ThrowsGraqlSyntaxException() {
        String template = "insert $x isa person has name <name> , has feet <numFeet> ";
        String expected = "insert $x0 has name \"Phil Collins\" isa person has feet 3;";

        Map<String, Object> data = new HashMap<>();
        data.put("name", "Phil Collins");
        data.put("feet", 3);

        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage(GraqlSyntaxException.parsingTemplateMissingKey("<numFeet>", data).getMessage());
        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateReplacesValueWithSpacedAsVariable_ReplacementHasDashesWhereSpacesWere(){
        String template = "insert $<address> has address <address>;";
        String expected = "insert $22--Hornsey has address \"22. Hornsey\";";


        Map<String, Object> data = new HashMap<>();
        data.put("address", "22. Hornsey");

        assertParseEquals(template, data, expected);
    }

    @Test
    public void noSpacesBetweenTokensTest(){
        String template =
                "insert " +
                        "for (whale in <whales>) do {" +
                        "\t\t\t$x isa whale has name <whale>;\n}";

        String expected =
                "insert $x0 isa whale has name \"shamu\";\n" +
                        "$x1 isa whale has name \"dory\";";

        Map<String, Object> data = new HashMap<>();
        data.put("whales", Arrays.asList("shamu", "dory"));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateDataContainsInt_ReplacedIntNotQuoted(){
        String template = "insert $x isa y val <value>;";
        String expected = "insert $x0 isa y val 40;";

        assertParseEquals(template, ImmutableMap.of("value", 40), expected);
    }

    @Test
    public void templateDataContainsString_ReplacedStringIsQuoted(){
        String template = "insert $x isa y val <value>;";
        String expected = "insert $x0 isa y val \"string\";";

        assertParseEquals(template, ImmutableMap.of("value", "string"), expected);
    }

    @Test
    public void templateDataContainsDouble_ReplacedDoubleNotQuoted(){
        String template = "insert $x isa y val <value>;";
        String expected = "insert $x0 isa y val 0.001;";

        assertParseEquals(template, ImmutableMap.of("value", 0.001), expected);
    }

    @Test
    public void templateDataContainsBoolean_ReplacedBooleanNotQuoted(){
        String template = "insert $x isa y val <value>;";
        String expected = "insert $x0 isa y val true;";

        assertParseEquals(template, ImmutableMap.of("value", true), expected);
    }

    @Test
    public void templateLoopsOverArrayWithForInSyntax_LoopIsExecutedForEachElementInArray(){
        String template = "insert " +
                "for (whale in <whales> ) do {" +
                "$x isa whale has name <whale>;\n}";

        String expected = "insert $x0 isa whale has name \"shamu\";\n" +
                                 "$x1 isa whale has name \"dory\";";

        assertParseEquals(template, ImmutableMap.of("whales",
                Arrays.asList("shamu", "dory")), expected);
    }

    @Test
    public void templateLoopsOverArrayOfMapsWithForEachSyntax_LoopIsExecutedForEachElementInArray(){
        String template = "insert\n" +
                "    $x isa person;\n" +
                "    for ( <addresses> ) do {\n" +
                "        $y isa address;\n" +
                "        $y has street <street> ;\n" +
                "        $y has number <houseNumber> ;\n" +
                "        ($x, $y) isa resides;\n" +
                "    }";

        String expected =
                "insert $x0 isa person;\n" +
                        "$y0 isa address;\n" +
                        "$y0 has street \"Collins Ave\";\n" +
                        "$y0 has number 8855;\n" +
                        "($x0, $y0) isa resides;\n" +
                        "$y1 isa address;\n" +
                        "$y1 has street \"Hornsey St\";\n" +
                        "$y1 has number 8;\n" +
                        "($x0, $y1) isa resides;";

        Map<String, Object> address1 = new HashMap<>();
        address1.put("street", "Collins Ave");
        address1.put("houseNumber", 8855);

        Map<String, Object> address2 = new HashMap<>();
        address2.put("street", "Hornsey St");
        address2.put("houseNumber", 8);

        Map<String, Object> data = new HashMap<>();
        data.put("addresses", Arrays.asList(address1, address2));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateLoopsOverArrayOfMapsWithForInSyntax_LoopIsExecutedForEachElementInArray(){
        String template = "insert\n" +
                "    $x isa person;\n" +
                "    for ( address in <addresses> ) do {\n" +
                "        $y isa address;\n" +
                "        $y has street <address.street> ;\n" +
                "        $y has number <address.number> ;\n" +
                "        ($x, $y) isa resides;\n" +
                "    }";

        String expected = "insert $x0 isa person;\n" +
                "$y0 isa address;\n" +
                "$y0 has street \"Collins Ave\";\n" +
                "$y0 has number 8855;\n" +
                "($x0, $y0) isa resides;\n" +
                "$y1 isa address;\n" +
                "$y1 has street \"Hornsey St\";\n" +
                "$y1 has number 8;\n" +
                "($x0, $y1) isa resides;";

        Map<String, Object> address1 = new HashMap<>();
        address1.put("street", "Collins Ave");
        address1.put("number", 8855);

        Map<String, Object> address2 = new HashMap<>();
        address2.put("street", "Hornsey St");
        address2.put("number", 8);

        Map<String, Object> data = new HashMap<>();
        data.put("addresses", Arrays.asList(address1, address2));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateLoopsOverArrayOfArraysWithForEachSyntax_LoopIsExecutedForEachElementInArray(){

        String template =
                "insert " +
                        "for ( <people> ) \n" +
                        "do { \n" +
                        "$x isa person has name <name>;\n" +
                        "    for ( <addresses> ) do {\n" +
                        "    $y isa address ;\n" +
                        "        $y has street <street> ;\n" +
                        "        $y has number <number> ;\n" +
                        "        ($x, $y) isa resides;\n" +
                        "    }\n" +
                        "}";

        String expected =
                "insert $x0 isa person has name \"Elmo\";\n" +
                        "$y0 isa address;\n" +
                        "$y0 has street \"North Pole\";\n" +
                        "$y0 has number 100;\n" +
                        "($x0, $y0) isa resides;\n" +
                        "$y1 isa address;\n" +
                        "$y1 has street \"South Pole\";\n" +
                        "$y1 has number -100;\n" +
                        "($x0, $y1) isa resides;\n" +
                        "$x1 isa person has name \"Flounder\";\n" +
                        "$y2 isa address;\n" +
                        "$y2 has street \"Under the sea\";\n" +
                        "$y2 has number 22;\n" +
                        "($x1, $y2) isa resides;";

        Map<String, Object> address1 = new HashMap<>();
        address1.put("street", "North Pole");
        address1.put("number", 100);

        Map<String, Object> address2 = new HashMap<>();
        address2.put("street", "South Pole");
        address2.put("number", -100);

        Map<String, Object> address3 = new HashMap<>();
        address3.put("street", "Under the sea");
        address3.put("number", 22);

        Map<String, Object> person1 = new HashMap<>();
        person1.put("name", "Elmo");
        person1.put("addresses", Arrays.asList(address1, address2));

        Map<String, Object> person2 = new HashMap<>();
        person2.put("name", "Flounder");
        person2.put("addresses", Collections.singletonList(address3));

        Map<String, Object> data = new HashMap<>();
        data.put("people", Arrays.asList(person1, person2));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateReusesVariablesAfterExitingInitialScope_VariableCounterIncremented(){
        String template =
                "insert $x isa person has name <name>;\n" +
                        "    \n" +
                        "for ( <addresses> ) do {\n" +
                        "        $y isa address;\n" +
                        "        $y has street <street> ;\n" +
                        "        $y has number <number> ;\n" +
                        "        ($x, $y) isa resides;\n" +
                        "}\n" +
                        "$y isa person;\n" +
                        "($x, $y) isa friends;";

        String expected =
                "insert $x0 isa person has name \"Manon\";\n" +
                        "$y0 isa address;\n" +
                        "$y0 has street \"Collins Ave\";\n" +
                        "$y0 has number 8855;\n" +
                        "($x0, $y0) isa resides;\n" +
                        "$y1 isa address;\n" +
                        "$y1 has street \"Hornsey St\";\n" +
                        "$y1 has number 8;\n" +
                        "($x0, $y1) isa resides;\n" +
                        "$y2 isa person;\n" +
                        "($x0, $y2) isa friends;";

        Map<String, Object> address1 = new HashMap<>();
        address1.put("street", "Collins Ave");
        address1.put("number", 8855);

        Map<String, Object> address2 = new HashMap<>();
        address2.put("street", "Hornsey St");
        address2.put("number", 8);

        Map<String, Object> data = new HashMap<>();
        data.put("addresses", Arrays.asList(address1, address2));
        data.put("name", "Manon");

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateUsesDotNotationToAccessMapElements_ElementsAreCorrectlyReplaced(){
        String template = "insert " +
                "$x isa person has name <name>;\n" +
                "$y isa address;\n" +
                "$y has street <address.street>;\n" +
                "$y has number <address.number>;\n" +
                "($x, $y) isa resides;";

        String expected = "insert $x0 has name \"Phil Collins\" isa person;\n" +
                "$y0 isa address;\n" +
                "$y0 has street \"Collins Ave\";\n" +
                "$y0 has number 1;\n" +
                "($x0, $y0) isa resides;";

        Map<String, Object> address = new HashMap<>();
        address.put("street", "Collins Ave");
        address.put("number", 1);

        Map<String, Object> data = new HashMap<>();
        data.put("name", "Phil Collins");
        data.put("address", address);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateUsesDoubleDotNotationToAccessMapElements_ElementsAreCorrectlyReplaced(){
        String template = "insert $x isa person has name <person.name.firstName>;\n";
        String expected = "insert $x0 isa person has name \"Phil\";";

        Map<String, Object> data = new HashMap<>();
        data.put("person", singletonMap("name", singletonMap("firstName", "Phil")));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateUsesQuotedKeysInDotNotationToAccessMapElements_ElementsAreCorrectlyReplaced(){
        String template = "insert $x isa person has name <\"person\".\"name\".\"firstName\">;";
        String expected = "insert $x0 isa person has name \"Phil\";";

        Map<String, Object> data = new HashMap<>();
        data.put("person", singletonMap("name", singletonMap("firstName", "Phil")));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateUsesDotNotationForReplacementInVariable_ElementsAreCorrectlyReplaced(){
        String template = "insert $<person.name> isa person;";
        String expected = "insert $Phil-Collins isa person;";

        Map<String, Object> data = new HashMap<>();
        data.put("person", singletonMap("name", "Phil Collins"));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateAccessesNonExistingDataElement_GraqlSyntaxExceptionThrown(){
        String template = "$<namefhwablfewqhbfli> isa person";
        String expected = "$Phil-Collins isa person";

        exception.expect(GraqlSyntaxException.class);

        assertParseEquals(template, ImmutableMap.of("this", "that"), expected);
    }

    @Test
    public void templateIfEvaluatesTrue_IfBlockAppearsInResult(){
        String template =
                "if(<firstName> = true) do { insert $person has hasName <firstName>; }\n" +
                        "elseif(<firstName> = false) do { insert $person isa person; }\n" +
                        "else { insert $nothing isa nothing; }";
        String expected = "insert $person0 has hasName true;";
        assertParseEquals(template, singletonMap("firstName", true), expected);
    }

    @Test
    public void templateElseIfEvaluatesTrue_ElseIfBlockAppearsInResult(){
        String template =
                "if(<firstName> = true)      do { insert $person isa if; }\n" +
                        "elseif(<firstName> = false) do { insert $person isa elseif; }\n" +
                        "else                           { insert $nothing isa else; }";
        String expected = "insert $nothing0 isa else;";
        assertParseEquals(template, singletonMap("firstName", "bleep"), expected);
    }

    @Test
    public void templateElseEvaluatesTrue_ElseBlockAppearsInResult(){
        String template =
                        "if(<firstName> = true)      do { insert $person isa if; }\n" +
                        "elseif(<firstName> = false) do { insert $person isa elseif; }\n" +
                        "else                           { insert $nothing isa else; }";
        String expected = "insert $nothing0 isa else;";
        assertParseEquals(template, singletonMap("firstName", "bleep"), expected);
    }

    @Test
    public void templateEvaluatesEqualityOverSameStrings_EqualityEvaluatesToTrue(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", "one");
        assertParseEquals("if(<first> = \"one\") do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
        assertParseEquals("if(<first> != \"one\") do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test
    public void templateEvaluatesAndExpressionOverTwoTrueBooleans_ExpressionEvaluatesToTrue(){
        String template = "if(<this> and <that>) do { insert $x isa t; } else { insert $x isa f; }";
        String expected = "insert $x0 isa t;";

        Map<String, Object> data = new HashMap<>();
        data.put("this", true);
        data.put("that", true);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateEvaluatesAndExpressionOverTrueAndFalseBooleans_ExpressionEvaluatesToFalse(){
        String template = "if(<this> and <that>) do { insert $x isa t; } else { insert $x isa f; }";
        String expected =  "insert $x0 isa f;";

        Map<String, Object> data = new HashMap<>();
        data.put("this", false);
        data.put("that", true);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateEvaluatesAndExpressionOverTwoFalseBooleans_ExpressionEvaluatesToFalse(){
        String template = "if(<this> and <that>) do { insert $x isa t; } else { insert $x isa f; }";
        String expected =  "insert $x0 isa f;";

        Map<String, Object> data = new HashMap<>();
        data.put("this", false);
        data.put("that", false);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateDataContainsWrongTypeForAndExpression_GraqlSyntaxExceptionThrown(){
        Map<String, Object> data = new HashMap<>();
        data.put("this", true);
        data.put("that", 2);

        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage(GraqlSyntaxException.parsingIncorrectValueType(2, Boolean.class, data).getMessage());
        assertParseEquals("if(<this> and <that>) do { something }", data, " something");
    }

    @Test
    public void templateEvaluatesOrExpressionOverTrueAndFalseBooleans_ExpressionEvaluatesToTrue(){
        String template = "if(<this> or <that>) do { insert $x isa t; } else { insert $x isa f; }";
        String expected = "insert $x0 isa t;";

        Map<String, Object> data = new HashMap<>();
        data.put("this", false);
        data.put("that", true);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateEvaluatesOrExpressionOverTwoTrueBooleans_ExpressionEvaluatesToTrue(){
        String template = "if(<this> or <that>) do { insert $x isa t; } else { insert $x isa f; }";
        String expected = "insert $x0 isa t;";

        Map<String, Object> data = new HashMap<>();
        data.put("this", true);
        data.put("that", true);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateEvaluatesOrExpressionOverTwoFalseBooleans_ExpressionEvaluatesToTrue(){
        String template = "if(<this> or <that>) do { insert $x isa t; } else { insert $x isa f; }";
        String expected = "insert $x0 isa f;";

        Map<String, Object> data = new HashMap<>();
        data.put("this", false);
        data.put("that", false);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void templateDataContainsWrongTypeForOfExpression_GraqlSyntaxExceptionThrown(){
        Map<String, Object> data = new HashMap<>();
        data.put("this", true);
        data.put("that", 2);

        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage(GraqlSyntaxException.parsingIncorrectValueType(2, Boolean.class, data).getMessage());
        assertParseEquals("if(<this> or <that>) do { something }", data, " something");
    }

    @Test
    public void templateEvaluatesOrExpressionOverFalseBoolean_ExpressionEvaluatesToTrue(){
        String template = "if(not <this>) do { insert $something isa something; } else { insert $something isa nothing; }";
        String expected = "insert $something0 isa something;";
        assertParseEquals(template, singletonMap("this", false), expected);
    }

    @Test
    public void templateEvaluatesOrExpressionOverTrueBoolean_ExpressionEvaluatesToFalse(){
        String template = "if(not <this>) do { insert $something isa something; } else { insert $something isa nothing; }";
        String expected = "insert $something0 isa nothing;";
        assertParseEquals(template, singletonMap("this", true), expected);
    }

    @Test
    public void templateDataContainsWrongTypeForNotExpression_GraqlSyntaxExceptionThrown(){
        Map data = singletonMap("this", "string");

        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage(GraqlSyntaxException.parsingIncorrectValueType("string", Boolean.class, data).getMessage());

        assertParseEquals("if(not <this>) do {insert isa y;} else {insert isa z;}", data, "");
    }

    @Test
    public void templateEvaluatesGreaterExpressionOverNumbers_ExpressionEvaluatesCorrectly(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", 2);

        assertParseEquals("if(<first> > <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
        assertParseEquals("if(<second> > <first>) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
    }

    @Test
    public void templateDataContainsWrongTypeForGreaterExpression_GraqlSyntaxExceptionThrown(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", "string");

        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage(GraqlSyntaxException.parsingIncorrectValueType("string", Number.class, data).getMessage());
        assertParseEquals("if(<first> > <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test
    public void templateEvaluatesGreaterEqualsExpressionOverNumbers_ExpressionEvaluatesCorrectly(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", 2);

        assertParseEquals("if(<first> >= <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
        assertParseEquals("if(<second> >= <first>) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");

        data.put("first", 2);
        assertParseEquals("if(<first> >= <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");

        data.put("first", 2.0);
        assertParseEquals("if(<first> >= <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
    }

    @Test
    public void templateDataContainsWrongTypeForGreaterEqualsExpression_GraqlSyntaxExceptionThrown(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", "string");

        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage(GraqlSyntaxException.parsingIncorrectValueType("string", Number.class, data).getMessage());
        assertParseEquals("if(<first> >= <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test
    public void templateEvaluatesLessExpressionOverNumbers_ExpressionEvaluatesCorrectly(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", 2);

        assertParseEquals("if(<first> < <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
        assertParseEquals("if(<second> < <first>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");

        data.put("second", 1);
        assertParseEquals("if(<second> < <first>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test
    public void templateDataContainsWrongTypeForLessExpression_GraqlSyntaxExceptionThrown(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", "string");

        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage(GraqlSyntaxException.parsingIncorrectValueType("string", Number.class, data).getMessage());
        assertParseEquals("if(<first> < <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test
    public void templateEvaluatesLessEqualsExpressionOverNumbers_ExpressionEvaluatesCorrectly(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", 2);

        assertParseEquals("if(<first> <= <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
        assertParseEquals("if(<second> <= <first>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");

        data.put("second", 2);
        assertParseEquals("if(<first> <= <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");

        data.put("second", 2.0);
        assertParseEquals("if(<first> <= <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
    }

    @Test
    public void templateDataContainsWrongTypeForLessEqualsExpression_GraqlSyntaxExceptionThrown(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", "string");

        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage(GraqlSyntaxException.parsingIncorrectValueType("string", Number.class, data).getMessage());
        assertParseEquals("if(<first> <= <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test
    public void templateGroupsAndExpressions_ExpressionEvaluatesCorrectly(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", 2);
        data.put("third", 3);

        assertParseEquals("if((<first> <= <second>) and (<second> <= <third>)) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
        assertParseEquals("if((<first> <= <second>) and (<third> <= <second>)) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test
    public void templateGroupsOrExpressions_ExpressionEvaluatesCorrectly(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", 2);
        data.put("third", 3);

        assertParseEquals("if((<first> <= <second>) or (<second> <= <third>)) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
        assertParseEquals("if((<first> <= <second>) or (<third> <= <second>)) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
        assertParseEquals("if((<second> <= <first>) or (<third> <= <second>)) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test
    public void templateGroupsAndOrExpressions_ExpressionEvaluatesCorrectly(){
        String template = "if (((false) and (false)) or (not ((true) and (false))))" +
                "do {insert isa y;} else {insert isa z;}";

        assertParseEquals(template, new HashMap<>(), "insert isa y;");
    }

    @Test
    public void templateHasNonAlphanumericSymbolInReplaceKey_ThrowsGraqlSyntaxException(){
        exception.expect(GraqlSyntaxException.class);

        String template = "insert $x isa person has name <First Name>;    ";
        String expected = "insert $x0 has name \"Phil Collins\" isa person;";

        assertParseEquals(template, singletonMap("First Name", "Phil Collins"), expected);
    }

    @Test
    public void templateHasQuotedNonAlphanumericSymbolInReplaceKey_TemplateEvaluatesCorrectly(){
        String template = "insert $x isa person has name <\"First Name\">;    ";
        String expected = "insert $x0 has name \"Phil Collins\" isa person;";

        assertParseEquals(template, singletonMap("First Name", "Phil Collins"), expected);
    }

    @Test
    public void templateIsInvalid_ThrowsGraqlSyntaxException(){
        exception.expect(GraqlSyntaxException.class);
        String template = "<<<<<<<";
        Graql.parser().parseTemplate(template, new HashMap<>()).forEach(q -> {});
    }

    @Test
    public void templateContainsStringInInDoubleQuotes_TemplateIsParsable(){
        String template = "insert $thing has quotes \"in quotes\";";
        String expected = "insert $thing0 has quotes \"in quotes\";";
        assertParseEquals(template, new HashMap<>(), expected);
    }

    @Test
    public void templateContainsSingleQuotesInDoubleQuotes_TemplateIsParsable(){
        String template = "insert thing has quotes \"'in' quotes\";";
        String expected = "insert label thing has quotes \"\\'in\\' quotes\";";
        assertParseEquals(template, new HashMap<>(), expected);
    }

    @Test
    public void templateContainsSingleQuotesInSingleQuotes_TemplateIsParsable(){
        String template = "insert thing has quotes '\"in\" quotes';";
        String expected = "insert has quotes \"\\\"in\\\" quotes\" label thing;";
        assertParseEquals(template, new HashMap<>(), expected);
    }

    @Test
    public void templateContainsEscapedDoubleQuotesInDoubleQuotes_TemplateIsParsable(){
        String template = "insert thing has quotes \"\\\"in\\\" quotes\";";
        String expected = "insert has quotes \"\\\"in\\\" quotes\" label thing;";
        assertParseEquals(template, new HashMap<>(), expected);
    }

    @Test
    public void templateWithMultipleQueries_IsCorrectlyParsed() {
        String template = "insert $x isa <type1>; insert $y isa <type2>;";

        String[] expected = {
                "insert $x0 isa thing1;",
                "insert $y0 isa thing2;"
        };

        Map<String, Object> data = new HashMap<>();
        data.put("type1", "thing1");
        data.put("type2", "thing2");

        assertParseContains(template, data, expected);
    }

    @Test
    public void whenGettingFirstItemInList_TemplateContainsFirstItem(){
        Map<String, Object> data = new HashMap<>();
        data.put("name", Arrays.asList("Alex", "Louise"));

        String template = "insert $this has name <name[0]>;";
        String expected = "insert $this0 has name \"Alex\";";
        assertParseEquals(template, data, expected);

        template = "insert $this has name <name[1]>;";
        expected = "insert $this0 has name \"Louise\";";
        assertParseEquals(template, data, expected);
    }

    @Test
    public void whenGettingIndexOutOfBoundsInList_ExceptionIsThrown(){
        List<String> list = Arrays.asList("Alex", "Louise");

        Map<String, Object> data = new HashMap<>();
        data.put("name", list);

        exception.expect(GraqlSyntaxException.class);
        exception.expectMessage(GraqlSyntaxException.create("Index [2] out of bounds for list "  + list).getMessage());

        String template = "insert $this has name <name[2]>;";
        String expected = "insert $this0 has name \"Alex\";";
        assertParseEquals(template, data, expected);
    }

    @Test
    public void whenParsingAMalformedTemplate_Throw() {
        // There is an extra `)` at the end
        String template = "for( <cars>) do { insert $x isa car has car_inventory_id <car_inventory_id>);}";

        Map<String, Object> data = ImmutableMap.of("cars", ImmutableList.of(ImmutableMap.of("car_inventory_id", 1)));

        exception.expect(GraqlSyntaxException.class);

        Graql.parser().parseTemplate(template, data);
    }

    private void assertParseContains(String template, Map<String, Object> data, String... expected){
        List<String> result = Graql.parser().parseTemplate(template, data).map(Query::toString).collect(toList());
        for(String e:expected){
            assertThat(result, hasItem(e));
        }
    }

    private void assertParseEquals(String template, Map<String, Object> data, String expected){
        List<Query> result = Graql.parser().parseTemplate(template, data).collect(toList());
        assertEquals(parse(expected), result.get(0));
    }
}
