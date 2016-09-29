/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
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
import static org.junit.Assert.assertTrue;

public class TemplateParserTest {

    private static TemplateParser parser;

    @BeforeClass
    public static void setup(){
        parser = TemplateParser.create();
    }

    @Test
    public void oneValueOneLineTest(){
        String template = "insert $x isa person has name <name>    ";
        String expected = "insert $x0 isa person has name \\\"Phil Collins\\\"    ";

        Map<String, Object> data = new HashMap<>();
        data.put("name", "Phil Collins");

        assertParseEquals(template, data, expected);
    }

    @Test
    public void multiValueOneLineTest(){
        String template = "insert $x isa person has name <name> , has feet <numFeet>";
        String expected = "insert $x0 isa person has name \\\"Phil Collins\\\" , has feet 3";

        Map<String, Object> data = new HashMap<>();
        data.put("name", "Phil Collins");
        data.put("numFeet", 3);

        assertParseEquals(template, data, expected);
    }

    @Test(expected = RuntimeException.class)
    public void dataMissingTest() {
        String template = "insert $x isa person has name <name> , has feet <numFeet> ";
        String expected = "insert $x0 isa person has name \\\"Phil Collins\\\", has feet 3 ";

        Map<String, Object> data = new HashMap<>();
        data.put("name", "Phil Collins");
        data.put("feet", 3);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void quotingWhenReplacementInVariableTest(){
        String template = "" +
               "insert \n" +
               "for {addresses} do { \n" +
               "   $<.> has address <.>;\n" +
               "}";

        String expected = "insert \n" +
                "   $22-Hornsey has address \\\"22 Hornsey\\\";\n" +
                "   $Something has address \\\"Something\\\";\n";


        Map<String, Object> data = new HashMap<>();
        data.put("addresses", Arrays.asList("22 Hornsey", "Something"));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void noSpacesBetweenTokensTest(){
        String template = "for {whales} do {" +
                "\t\t\t$x isa whale has name, <.>;\n}";

        String expected =
                        "\t\t\t$x0 isa whale has name, \\\"shamu\\\";\n" +
                        "\t\t\t$x1 isa whale has name, \\\"dory\\\";\n";

        Map<String, Object> data = new HashMap<>();
        data.put("whales", Arrays.asList("shamu", "dory"));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void multipleDataTypesTest(){
        String template = "first is a <string>, second a <long>, third a <double>, fourth a <bool>";
        String expected = "first is a \\\"string\\\", second a 40, third a 0.001, fourth a false";

        Map<String, Object> data = new HashMap<>();
        data.put("string", "string");
        data.put("long", 40);
        data.put("double", 0.001);
        data.put("bool", false);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void forLoopOverArrayTest(){
        String template = "for { whales } do {" +
                "$x isa whale has name <.>;\n}";

        String expected =
                "$x0 isa whale has name \\\"shamu\\\";\n" +
                "$x1 isa whale has name \\\"dory\\\";\n";

        Map<String, Object> data = new HashMap<>();
        data.put("whales", Arrays.asList("shamu", "dory"));

        assertParseEquals(template, data, expected);
    }

    @Test
    public void forLoopOverObjectsTest(){
        String template = "insert\n" +
                "    $x isa person;\n" +
                "    for { addresses } do {\n" +
                "        $y isa address;\n" +
                "        $y has street <street> ;\n" +
                "        $y has number <houseNumber> ;\n" +
                "        ($x, $y) isa resides;\n" +
                "    }";

        String expected = "insert\n" +
                "    $x0 isa person;\n" +
                "        $y0 isa address;\n" +
                "        $y0 has street \\\"Collins Ave\\\" ;\n" +
                "        $y0 has number 8855 ;\n" +
                "        ($x0, $y0) isa resides;\n" +
                "        $y1 isa address;\n" +
                "        $y1 has street \\\"Hornsey St\\\" ;\n" +
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
    public void doubleNestedForTest(){

        String template = "" +
                "for { people } \n" +
                "do { \n" +
                "insert $x isa person has name <name>;\n" +
                "    for { addresses } do {\n" +
                "    insert $y isa address ;\n" +
                "        $y has street <street> ;\n" +
                "        $y has number <number> ;\n" +
                "        ($x, $y) isa resides;\n" +
                "    }\n" +
                "}";

        String expected = "" +
                "insert $x0 isa person has name \\\"Elmo\\\";\n" +
                "    insert $y0 isa address ;\n" +
                "        $y0 has street \\\"North Pole\\\" ;\n" +
                "        $y0 has number 100 ;\n" +
                "        ($x0, $y0) isa resides;\n" +
                "    insert $y1 isa address ;\n" +
                "        $y1 has street \\\"South Pole\\\" ;\n" +
                "        $y1 has number -100 ;\n" +
                "        ($x0, $y1) isa resides;\n" +
                "insert $x1 isa person has name \\\"Flounder\\\";\n" +
                "    insert $y2 isa address ;\n" +
                "        $y2 has street \\\"Under the sea\\\" ;\n" +
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
    public void dotNotationTest(){
        String template = "" +
                "$x isa person has name <name>;\n" +
                "$y isa address;\n" +
                "$y has street <address.street>;\n" +
                "$y has number <address.number>;\n" +
                "($x, $y) isa resides;";

        String expected = "" +
                "$x0 isa person has name \\\"Phil Collins\\\";\n" +
                "$y0 isa address;\n" +
                "$y0 has street \\\"Collins Ave\\\";\n" +
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
        String expected = "$x0 isa person has name \\\"Phil\\\"\n";

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
    public void ifElseTest(){
        String template = "" +
                "if { firstName } do {\n" +
                "    insert $person has name <firstName>;" +
                "}" +
                "else {" +
                "    insert $person;" +
                "}";
        String expected = "    insert $person0 has name \\\"Phil\\\";";

        Map<String, Object> data = new HashMap<>();
        data.put("firstName", "Phil");

        assertParseEquals(template, data, expected);

        expected = "    insert $person0;";
        data = new HashMap<>();

        assertParseEquals(template, data, expected);
    }

    private void assertParseEquals(String template, Map<String, Object> data, String expected){
        String result = parser.parseTemplate(template, data);
        System.out.println(result);
        assertEquals(expected, result);
    }
}
