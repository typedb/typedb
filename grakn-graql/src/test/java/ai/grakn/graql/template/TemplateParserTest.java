/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.graql.template;

import ai.grakn.exception.GraqlTemplateParsingException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Query;
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
    public void oneValueOneLineTest(){
        String template = "insert $x isa person has name <name>;    ";
        String expected = "insert $x0 has name \"Phil Collins\" isa person;";

        Map<String, Object> data = new HashMap<>();
        data.put("name", "Phil Collins");

        assertParseEquals(template, data, expected);
    }

    @Test
    public void concatenateValuesTest(){
        String template = "insert $x isa @noescp(<first>)-@noescp(<last>);";
        String expected = "insert $x0 isa one-two;";

        Map<String, Object> data = new HashMap<>();
        data.put("first", "one");
        data.put("last", "two");

        assertParseEquals(template, data, expected);
    }

    @Test
    public void multiValueOneLineTest(){
        String template = "insert $x isa person has name <name> , has feet <numFeet>;";
        String expected = "insert $x0 has name \"Phil Collins\" isa person has feet 3;";

        Map<String, Object> data = new HashMap<>();
        data.put("name", "Phil Collins");
        data.put("numFeet", 3);

        assertParseEquals(template, data, expected);
    }

    @Test(expected = GraqlTemplateParsingException.class)
    public void dataMissingTest() {
        String template = "insert $x isa person has name <name> , has feet <numFeet> ";
        String expected = "insert $x0 has name \"Phil Collins\" isa person has feet 3;";

        Map<String, Object> data = new HashMap<>();
        data.put("name", "Phil Collins");
        data.put("feet", 3);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void quotingWhenReplacementInVariableTest(){
        String template =
                "insert \n" +
                        "for (address in <addresses>) do { \n" +
                        "   $<address> has address <address>;\n" +
                        "}";

        String expected = "insert $22--Hornsey has address \"22. Hornsey\";\n" +
                "$Something has address \"Something\";";


        Map<String, Object> data = new HashMap<>();
        data.put("addresses", Arrays.asList("22. Hornsey", "Something"));

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
    public void multipleDataTypesTest(){
        String template = "insert $x isa y; $x val <string>; $x val <long>; $x val <double>; $x val <bool>;";
        String expected =
                "insert $x0 isa y;\n" +
                        "$x0 val \"string\";\n" +
                        "$x0 val 40;\n" +
                        "$x0 val 0.001;\n" +
                        "$x0 val false;";

        Map<String, Object> data = new HashMap<>();
        data.put("string", "string");
        data.put("long", 40);
        data.put("double", 0.001);
        data.put("bool", false);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void forLoopOverArrayTest(){
        String template = "insert " +
                "for (whale in <whales> ) do {" +
                "$x isa whale has name <whale>;\n}";

        String expected =
                "insert $x0 isa whale has name \"shamu\";\n" +
                        "$x1 isa whale has name \"dory\";";

        Map<String, Object> data = new HashMap<>();
        data.put("whales", Arrays.asList("shamu", "dory"));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void forLoopOverObjectsWithEnhancedForSyntaxTest(){
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
    public void forLoopOverObjectsWithNormalForSyntaxTest(){
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
    public void doubleNestedForTest(){

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
    public void reusingVariablesAfterBlockScopingTest(){
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
    public void dotNotationTest(){
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
    public void doubleDotTest(){
        String template = "insert $x isa person has name <person.name.firstName>;\n";
        String expected = "insert $x0 isa person has name \"Phil\";";

        Map<String, Object> data = new HashMap<>();
        data.put("person", singletonMap("name", singletonMap("firstName", "Phil")));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void comboVarDotTest(){
        String template = "insert $<person.name> isa person;";
        String expected = "insert $Phil-Collins isa person;";

        Map<String, Object> data = new HashMap<>();
        data.put("person", singletonMap("name", "Phil Collins"));

        assertParseEquals(template, data, expected);
    }

    @Test(expected = GraqlTemplateParsingException.class)
    public void wrongDataTest(){
        String template = "$<person.namefhwablfewqhbfli> isa person";
        String expected = "$Phil-Collins isa person";

        Map<String, Object> data = new HashMap<>();
        data.put("person", singletonMap("name", "Phil Collins"));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void ifElseIfTest(){
        String template =
                "if(<firstName> = true) do { insert $person has hasName <firstName>; }\n" +
                        "elseif(<firstName> = false) do { insert $person isa person; }\n" +
                        "else { insert $nothing isa nothing; }";
        String expected = "insert $person0 has hasName true;";

        assertParseEquals(template, singletonMap("firstName", true), expected);

        expected = "insert $person0 isa person;";
        assertParseEquals(template, singletonMap("firstName", false), expected);

        expected = "insert $nothing0 isa nothing;";
        assertParseEquals(template, singletonMap("firstName", "bleep"), expected);
    }

    @Test
    public void equalityWithStringTest(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", "one");
        assertParseEquals("if(<first> = \"one\") do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
        assertParseEquals("if(<first> != \"one\") do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test
    public void ifElseTest(){
        String template =
                "if (<firstName> != null) do {\n" +
                        "    insert $person has name <firstName>;" +
                        "}\n" +
                        "else {\n" +
                        "    insert $person;" +
                        "}\n";
        String expected = "insert $person0 has name \"Phil\";";

        assertParseEquals(template, singletonMap("firstName", "Phil"), expected);

        expected = "insert $person0 ;";

        assertParseEquals(template, new HashMap<>(), expected);
    }

    @Test
    public void andExpressionTest(){
        String template = "if(<this> and <that>) do { insert $x isa t; } else { insert $x isa f; }";
        String expected = "insert $x0 isa t;";

        Map<String, Object> data = new HashMap<>();
        data.put("this", true);
        data.put("that", true);

        assertParseEquals(template, data, expected);

        expected = "insert $x0 isa f;";
        data = new HashMap<>();
        data.put("this", false);
        data.put("that", true);

        assertParseEquals(template, data, expected);

        expected = "insert $x0 isa f;";
        data = new HashMap<>();
        data.put("this", false);
        data.put("that", false);

        assertParseEquals(template, data, expected);
    }

    @Test(expected = GraqlTemplateParsingException.class)
    public void andExpressionWrongTypeTest(){
        Map<String, Object> data = new HashMap<>();
        data.put("this", true);
        data.put("that", 2);
        assertParseEquals("if(<this> and <that>) do { something }", data, " something");
    }

    @Test
    public void orExpressionTest(){
        String template = "if(<this> or <that>) do { insert $x isa t; } else { insert $x isa f; }";
        String expected = "insert $x0 isa t;";

        Map<String, Object> data = new HashMap<>();
        data.put("this", false);
        data.put("that", true);

        assertParseEquals(template, data, expected);

        expected = "insert $x0 isa t;";
        data = new HashMap<>();
        data.put("this", true);
        data.put("that", true);

        assertParseEquals(template, data, expected);

        expected = "insert $x0 isa f;";
        data = new HashMap<>();
        data.put("this", false);
        data.put("that", false);

        assertParseEquals(template, data, expected);
    }

    @Test(expected = GraqlTemplateParsingException.class)
    public void orExpressionWrongTypeTest(){
        Map<String, Object> data = new HashMap<>();
        data.put("this", true);
        data.put("that", 2);
        assertParseEquals("if(<this> or <that>) do { something }", data, " something");
    }

    @Test
    public void notExpressionTest(){
        String template = "if(not <this>) do { insert $something isa something; } else { insert $something isa nothing; }";
        String expected = "insert $something0 isa something;";

        assertParseEquals(template, singletonMap("this", false), expected);

        expected = "insert $something0 isa nothing;";
        assertParseEquals(template, singletonMap("this", true), expected);
    }

    @Test(expected = GraqlTemplateParsingException.class)
    public void notExpressionWrongTypeTest(){
        assertParseEquals("if(not <this>) do {insert isa y;} else {insert isa z;}", singletonMap("this", "string"), "");
    }

    @Test
    public void greaterExpressionTest(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", 2);

        assertParseEquals("if(<first> > <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
        assertParseEquals("if(<second> > <first>) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
    }

    @Test(expected = GraqlTemplateParsingException.class)
    public void greaterExpressionWrongTypeTest(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", "string");
        assertParseEquals("if(<first> > <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test
    public void greaterEqualsExpressionTest(){
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

    @Test(expected = GraqlTemplateParsingException.class)
    public void greaterEqualsExpressionWrongTypeTest(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", "string");
        assertParseEquals("if(<first> >= <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test
    public void lessExpressionTest(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", 2);

        assertParseEquals("if(<first> < <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
        assertParseEquals("if(<second> < <first>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");

        data.put("second", 1);
        assertParseEquals("if(<second> < <first>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test(expected = GraqlTemplateParsingException.class)
    public void lessExpressionWrongTypeTest(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", "string");
        assertParseEquals("if(<first> < <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test
    public void lessEqualsExpressionTest(){
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

    @Test(expected = GraqlTemplateParsingException.class)
    public void lessEqualsExpressionWrongTypeTest(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", "string");
        assertParseEquals("if(<first> <= <second>) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test
    public void concatReplaceTest(){
        String template = "insert $@noescp(<pokemon_id>)-pokemon isa pokemon;\n$@noescp(<type_id>)-type isa pokemon-type;";
        String expected = "insert $124-pokemon isa pokemon;\n$124-type isa pokemon-type;";

        Map<String, Object> data = new HashMap<>();
        data.put("pokemon_id", 124);
        data.put("type_id", 124);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void andGroupExpressionTest(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", 2);
        data.put("third", 3);

        assertParseEquals("if((<first> <= <second>) and (<second> <= <third>)) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
        assertParseEquals("if((<first> <= <second>) and (<third> <= <second>)) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test
    public void orGroupExpressionTest(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", 2);
        data.put("third", 3);

        assertParseEquals("if((<first> <= <second>) or (<second> <= <third>)) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
        assertParseEquals("if((<first> <= <second>) or (<third> <= <second>)) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
        assertParseEquals("if((<second> <= <first>) or (<third> <= <second>)) do {insert isa y;} else {insert isa z;}", data, "insert isa z;");
    }

    @Test
    public void complicatedGroupExpressionTest(){
        String template = "if (((false) and (false)) or (not ((true) and (false))))" +
                "do {insert isa y;} else {insert isa z;}";

        assertParseEquals(template, new HashMap<>(), "insert isa y;");
    }

    @Test
    public void macroGroupExpressionTest(){
        Map<String, Object> data = new HashMap<>();
        data.put("first", 1);
        data.put("second", 2);
        data.put("third", 3);

        assertParseEquals("if((not @equals(<first>, <second>)) and @equals(<third>, <third>)) do {insert isa y;} else {insert isa z;}", data, "insert isa y;");
    }

    @Test
    public void keyWithSpacesFailsTest(){
        exception.expect(IllegalArgumentException.class);

        String template = "insert $x isa person has name <First Name>;    ";
        String expected = "insert $x0 has name \"Phil Collins\" isa person;";

        assertParseEquals(template, singletonMap("First Name", "Phil Collins"), expected);
    }

    @Test
    public void keyWithSpacesInQuotesTest(){


        String template = "insert $x isa person has name <\"First Name\">;    ";
        String expected = "insert $x0 has name \"Phil Collins\" isa person;";

        assertParseEquals(template, singletonMap("First Name", "Phil Collins"), expected);
    }

    @Test
    public void testGraqlParsingException(){
        exception.expect(IllegalArgumentException.class);
        String template = "<<<<<<<";
        Graql.parseTemplate(template, new HashMap<>());
    }

    @Test
    public void quotesTest(){
        String template = "insert $thing has quotes \"in quotes\";";
        String expected = "insert $thing0 has quotes \"in quotes\";";
        assertParseEquals(template, new HashMap<>(), expected);
    }

    @Test
    public void doubleQuotesInSingleQuotesTest(){
        String template = "insert thing has quotes \"'in' quotes\";";
        String expected = "insert label thing has quotes \"\\'in\\' quotes\";";
        assertParseEquals(template, new HashMap<>(), expected);
    }

    @Test
    public void singleQuotesInSingleQuotesTest(){
        String template = "insert thing has quotes '\"in\" quotes';";
        String expected = "insert has quotes \"\\\"in\\\" quotes\" label thing;";
        assertParseEquals(template, new HashMap<>(), expected);
    }

    @Test
    public void escapedDoubleQuotesInDoubleQuotesTest(){
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

    private void assertParseContains(String template, Map<String, Object> data, String... expected){
        List<String> result = Graql.parseTemplate(template, data).stream().map(Query::toString).collect(toList());
        for(String e:expected){
            assertThat(result, hasItem(e));
        }
    }

    private void assertParseEquals(String template, Map<String, Object> data, String expected){
        List<Query> result = Graql.parseTemplate(template, data);
        assertEquals(parse(expected), result.get(0));
    }
}
