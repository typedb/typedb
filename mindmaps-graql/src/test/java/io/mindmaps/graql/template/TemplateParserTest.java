/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research  Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.template;

import io.mindmaps.graql.internal.template.TemplateParser;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static junit.framework.TestCase.assertEquals;

public class TemplateParserTest {

    private static TemplateParser parser;

    @BeforeClass
    public static void setup(){
        parser = TemplateParser.create();
    }

    @Test
    public void oneValueOneLineTest(){
        String template = "insert $x isa person has name <name>    ";
        String expected = "insert $x0 isa person has name \"Phil Collins\"    ";

        Map<String, Object> data = new HashMap<>();
        data.put("name", "Phil Collins");

        assertParseEquals(template, data, expected);
    }

    @Test
    public void multiValueOneLineTest(){
        String template = "insert $x isa person has name <name> , has feet <numFeet>";
        String expected = "insert $x0 isa person has name \"Phil Collins\" , has feet 3";

        Map<String, Object> data = new HashMap<>();
        data.put("name", "Phil Collins");
        data.put("numFeet", 3);

        assertParseEquals(template, data, expected);
    }

    @Test(expected = RuntimeException.class)
    public void dataMissingTest() {
        String template = "insert $x isa person has name <name> , has feet <numFeet> ";
        String expected = "insert $x0 isa person has name \"Phil Collins\", has feet 3 ";

        Map<String, Object> data = new HashMap<>();
        data.put("name", "Phil Collins");
        data.put("feet", 3);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void quotingWhenReplacementInVariableTest(){
        String template = "" +
                "insert \n" +
                "for (address in addresses) do { \n" +
                "   $<address> has address <address>;\n" +
                "}";

        String expected = "insert \n" +
                "   $22-Hornsey has address \"22 Hornsey\";\n" +
                "   $Something has address \"Something\";\n";


        Map<String, Object> data = new HashMap<>();
        data.put("addresses", Arrays.asList("22 Hornsey", "Something"));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void noSpacesBetweenTokensTest(){
        String template = "for (whale in whales) do {" +
                "\t\t\t$x isa whale has name, <whale>;\n}";

        String expected =
                "\t\t\t$x0 isa whale has name, \"shamu\";\n" +
                        "\t\t\t$x1 isa whale has name, \"dory\";\n";

        Map<String, Object> data = new HashMap<>();
        data.put("whales", Arrays.asList("shamu", "dory"));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void multipleDataTypesTest(){
        String template = "first is a <string>, second a <long>, third a <double>, fourth a <bool>";
        String expected = "first is a \"string\", second a 40, third a 0.001, fourth a false";

        Map<String, Object> data = new HashMap<>();
        data.put("string", "string");
        data.put("long", 40);
        data.put("double", 0.001);
        data.put("bool", false);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void forLoopOverArrayTest(){
        String template = "for (whale in whales ) do {" +
                "$x isa whale has name <whale>;\n}";

        String expected =
                "$x0 isa whale has name \"shamu\";\n" +
                        "$x1 isa whale has name \"dory\";\n";

        Map<String, Object> data = new HashMap<>();
        data.put("whales", Arrays.asList("shamu", "dory"));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void forLoopOverObjectsWithEnhancedForSyntaxTest(){
        String template = "insert\n" +
                "    $x isa person;\n" +
                "    for ( addresses ) do {\n" +
                "        $y isa address;\n" +
                "        $y has street <street> ;\n" +
                "        $y has number <houseNumber> ;\n" +
                "        ($x, $y) isa resides;\n" +
                "    }";

        String expected = "insert\n" +
                "    $x0 isa person;\n" +
                "        $y0 isa address;\n" +
                "        $y0 has street \"Collins Ave\" ;\n" +
                "        $y0 has number 8855 ;\n" +
                "        ($x0, $y0) isa resides;\n" +
                "        $y1 isa address;\n" +
                "        $y1 has street \"Hornsey St\" ;\n" +
                "        $y1 has number 8 ;\n" +
                "        ($x0, $y1) isa resides;\n";

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
                "    for ( address in addresses ) do {\n" +
                "        $y isa address;\n" +
                "        $y has street <address.street> ;\n" +
                "        $y has number <address.number> ;\n" +
                "        ($x, $y) isa resides;\n" +
                "    }";

        String expected = "insert\n" +
                "    $x0 isa person;\n" +
                "        $y0 isa address;\n" +
                "        $y0 has street \"Collins Ave\" ;\n" +
                "        $y0 has number 8855 ;\n" +
                "        ($x0, $y0) isa resides;\n" +
                "        $y1 isa address;\n" +
                "        $y1 has street \"Hornsey St\" ;\n" +
                "        $y1 has number 8 ;\n" +
                "        ($x0, $y1) isa resides;\n";

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

        String template = "" +
                "for ( people ) \n" +
                "do { \n" +
                "insert $x isa person has name <name>;\n" +
                "    for ( addresses ) do {\n" +
                "    insert $y isa address ;\n" +
                "        $y has street <street> ;\n" +
                "        $y has number <number> ;\n" +
                "        ($x, $y) isa resides;\n" +
                "    }\n" +
                "}";

        String expected = "" +
                "insert $x0 isa person has name \"Elmo\";\n" +
                "    insert $y0 isa address ;\n" +
                "        $y0 has street \"North Pole\" ;\n" +
                "        $y0 has number 100 ;\n" +
                "        ($x0, $y0) isa resides;\n" +
                "    insert $y1 isa address ;\n" +
                "        $y1 has street \"South Pole\" ;\n" +
                "        $y1 has number -100 ;\n" +
                "        ($x0, $y1) isa resides;\n" +
                "insert $x1 isa person has name \"Flounder\";\n" +
                "    insert $y2 isa address ;\n" +
                "        $y2 has street \"Under the sea\" ;\n" +
                "        $y2 has number 22 ;\n" +
                "        ($x1, $y2) isa resides;\n";

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
        String template = "" +
                "insert $x isa person has name <name>;\n" +
                "    \n" +
                "for ( addresses ) do {\n" +
                "        $y isa address;\n" +
                "        $y has street <street> ;\n" +
                "        $y has number <number> ;\n" +
                "        ($x, $y) isa resides;\n" +
                "}\n" +
                "$y isa person, id 1234;\n" +
                "($x, $y) isa friends;";

        String expected = "" +
                "insert $x0 isa person has name \"Manon\";\n" +
                "        $y0 isa address;\n" +
                "        $y0 has street \"Collins Ave\" ;\n" +
                "        $y0 has number 8855 ;\n" +
                "        ($x0, $y0) isa resides;\n" +
                "        $y1 isa address;\n" +
                "        $y1 has street \"Hornsey St\" ;\n" +
                "        $y1 has number 8 ;\n" +
                "        ($x0, $y1) isa resides;\n" +
                "$y2 isa person, id 1234;\n" +
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
        String template = "" +
                "$x isa person has name <name>;\n" +
                "$y isa address;\n" +
                "$y has street <address.street>;\n" +
                "$y has number <address.number>;\n" +
                "($x, $y) isa resides;";

        String expected = "" +
                "$x0 isa person has name \"Phil Collins\";\n" +
                "$y0 isa address;\n" +
                "$y0 has street \"Collins Ave\";\n" +
                "$y0 has number 1;\n" +
                "($x0, $y0) isa resides;";

        Map<String, Object> address = new HashMap<>();
        address.put("street", "Collins Ave");
        address.put("number", 01);

        Map<String, Object> data = new HashMap<>();
        data.put("name", "Phil Collins");
        data.put("address", address);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void doubleDotTest(){
        String template = "$x isa person has name <person.name.firstName>\n";
        String expected = "$x0 isa person has name \"Phil\"\n";

        Map<String, Object> data = new HashMap<>();
        data.put("person", Collections.singletonMap("name", Collections.singletonMap("firstName", "Phil")));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void comboVarDotTest(){
        String template = "$<person.name> isa person";
        String expected = "$Phil-Collins isa person";

        Map<String, Object> data = new HashMap<>();
        data.put("person", Collections.singletonMap("name", "Phil Collins"));

        assertParseEquals(template, data, expected);
    }

    @Test(expected = RuntimeException.class)
    public void wrongDataTest(){
        String template = "$<person.namefhwablfewqhbfli> isa person";
        String expected = "$Phil-Collins isa person";

        Map<String, Object> data = new HashMap<>();
        data.put("person", Collections.singletonMap("name", "Phil Collins"));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void ifElseIfTest(){
        String template = "" +
                "if(firstName == true ) do { insert $person has hasName <firstName>; }\n" +
                "elseif(firstName == false) do { insert $person; }\n" +
                "else { something }";
        String expected = " insert $person0 has hasName true;";

        assertParseEquals(template, Collections.singletonMap("firstName", true), expected);

        expected = " insert $person0;";
        assertParseEquals(template, Collections.singletonMap("firstName", false), expected);

        expected = " something";
        assertParseEquals(template, new HashMap<>(), expected);
    }

    @Test
    public void ifElseTest(){
        String template = "" +
                "if ( firstName != null ) do {\n" +
                "    insert $person has name <firstName>;" +
                "}\n" +
                "else {\n" +
                "    insert $person;" +
                "}\n";
        String expected = "    insert $person0 has name \"Phil\";";

        Map<String, Object> data = new HashMap<>();
        data.put("firstName", "Phil");

        assertParseEquals(template, data, expected);

        expected = "    insert $person0;";
        data = new HashMap<>();

        assertParseEquals(template, data, expected);
    }

    @Test
    public void andExpressionTest(){
        String template = "if(this and that) do { something }";
        String expected = " something";

        Map<String, Object> data = new HashMap<>();
        data.put("this", true);
        data.put("that", true);

        assertParseEquals(template, data, expected);

        expected = "";
        data = new HashMap<>();
        data.put("this", false);
        data.put("that", true);

        assertParseEquals(template, data, expected);

        expected = "";
        data = new HashMap<>();
        data.put("this", false);
        data.put("that", false);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void orExpressionTest(){
        String template = "if(this or that) do { something }";
        String expected = " something";

        Map<String, Object> data = new HashMap<>();
        data.put("this", false);
        data.put("that", true);

        assertParseEquals(template, data, expected);

        expected = " something";
        data = new HashMap<>();
        data.put("this", true);
        data.put("that", true);

        assertParseEquals(template, data, expected);

        expected = "";
        data = new HashMap<>();
        data.put("this", false);
        data.put("that", false);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void notExpressionTest(){
        String template = "if(not this) do { something }";
        String expected = " something";

        assertParseEquals(template, Collections.singletonMap("this", false), expected);

        expected = "";
        assertParseEquals(template, Collections.singletonMap("this", true), expected);
    }

    @Test
    public void concatReplaceTest(){
        String template = "(pokemon-with-type: <pokemon_id>-pokemon, type-of-pokemon: <type_id>-type) isa has-type;";
        String expected = "(pokemon-with-type: \"124-pokemon\", type-of-pokemon: \"124-type\") isa has-type;";

        Map<String, Object> data = new HashMap<>();
        data.put("pokemon_id", 124);
        data.put("type_id", 124);

        assertParseEquals(template, data, expected);
    }

    private void assertParseEquals(String template, Map<String, Object> data, String expected){
        System.out.println(template);
        System.out.println();
        String result = parser.parseTemplate(template, data);
        System.out.println(result);
        assertEquals(expected, result);
    }
}
