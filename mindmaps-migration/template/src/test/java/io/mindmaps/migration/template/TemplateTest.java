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

package io.mindmaps.migration.template;

import mjson.Json;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class TemplateTest {

    private static TemplateParser parser;

    @BeforeClass
    public static void setup(){
        parser = new TemplateParser();
    }

    @Test
    public void oneValueOneLineTest(){
        String template = "insert $x isa person has name <name>    ";
        String expected = "insert $x0 isa person has name \\\"Phil Collins\\\"    ";

        String json = "{\"name\" : \"Phil Collins\"}";
        assertParseEquals(template, json, expected);
    }

    @Test
    public void multiValueOneLineTest(){
        String template = "insert $x isa person has name <name> , has feet <numFeet>";
        String expected = "insert $x0 isa person has name \\\"Phil Collins\\\" , has feet 3";

        String json = "{\"name\" : \"Phil Collins\", \"numFeet\":3}";
        assertParseEquals(template, json, expected);
    }

    @Test(expected = RuntimeException.class)
    public void dataMissingTest() {
        String template = "insert $x isa person has name <name> , has feet <numFeet> ";
        String expected = "insert $x0 isa person has name \\\"Phil Collins\\\", has feet 3 ";

        String json = "{\"name\" : \"Phil Collins\", \"feet\":3}";
        assertParseEquals(template, json, expected);
    }

    @Test
    public void quotingWhenReplacementInVariableTest(){
       String template = "" +
               "insert \n" +
               "for {addresses} do { \n" +
               "   $<.> has address <.>;\n" +
               "}";

        String json = "" +
                "{" +
                "   \"addresses\":[" +
                "       \"22 Hornsey\"," +
                "       \"Something\"" +
                "   ]" +
                "}";

        String expected = "insert \n" +
                "   $22-Hornsey has address \\\"22 Hornsey\\\";\n" +
                "   $Something has address \\\"Something\\\";\n";

        assertParseEquals(template, json, expected);
    }

    @Test
    public void noSpacesBetweenTokensTest(){
        String template = "for {whales} do {" +
                "\t\t\t$x isa whale has name, <.>;\n}";

        String json = "{\"whales\": [" +
                "\"shamu\"," +
                "\"dory\"" +
                "]}";

        String expected =
                "\t\t\t$x0 isa whale has name, \\\"shamu\\\";\n" +
                        "\t\t\t$x1 isa whale has name, \\\"dory\\\";\n";

        assertParseEquals(template, json, expected);
    }

    @Test
    public void multipleDataTypesTest(){
        String template = "first is a <string>, second a <long>, third a <double>, fourth a <bool>";
        String expected = "first is a \\\"string\\\", second a 40, third a 0.001, fourth a false";

        String json = "{" +
                "\"string\" : \"string\", " +
                "\"long\" : 40, " +
                "\"double\" : 0.001, " +
                "\"bool\": false}";

        assertParseEquals(template, json, expected);
    }

    @Test
    public void forLoopOverArrayTest(){
        String template = "for { whales } do {" +
                "$x isa whale has name <.>;\n}";

        String json = "{\"whales\": [" +
                "\"shamu\"," +
                "\"dory\"" +
                "]}";

        String expected =
                "$x0 isa whale has name \\\"shamu\\\";\n" +
                "$x1 isa whale has name \\\"dory\\\";\n";

        assertParseEquals(template, json, expected);
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

        String json = "{\n" +
                "    \"addresses\" : [\n" +
                "        {\n" +
                "            \"street\" : \"Collins Ave\",\n" +
                "            \"houseNumber\": 8855\n" +
                "        },\n" +
                "        {\n" +
                "            \"street\" : \"Hornsey St\",\n" +
                "            \"houseNumber\" : 8\n" +
                "        }\n" +
                "    ]\n" +
                "}";

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

        assertParseEquals(template, json, expected);
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

        String json = "{\n" +
                "    \"people\" : [\n" +
                "        {\n" +
                "            \"name\" : \"Elmo\",\n" +
                "            \"addresses\" : [\n" +
                "                {\n" +
                "                    \"street\" : \"North Pole\",\n" +
                "                    \"number\" : 100\n" +
                "                },\n" +
                "                {\n" +
                "                    \"street\" : \"South Pole\",\n" +
                "                    \"number\" : -100\n" +
                "                }\n" +
                "            ]\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\" : \"Flounder\",\n" +
                "            \"addresses\" : [\n" +
                "                {\n" +
                "                    \"street\" : \"Under the sea\",\n" +
                "                    \"number\" : 22\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
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

        assertParseEquals(template, json, expected);
    }

    @Test
    public void unsupportedTypeExceptionTest(){
        assertTrue(false);
    }

    @Test
    public void dotNotationTest(){
        String template = "" +
                "$x isa person has name <name>;\n" +
                "$y isa address;\n" +
                "$y has street <address.street>;\n" +
                "$y has number <address.number>;\n" +
                "($x, $y) isa resides;";

        String json = "" +
                "{\n" +
                "\t\"name\" : \"Phil Collins\",\n" +
                "\t\"address\" : {\n" +
                "\t\t\"street\": \"Collins Ave\",\n" +
                "\t\t\"number\": 01\n" +
                "\t}\n" +
                "}\n";

        String expected = "" +
                "$x0 isa person has name \\\"Phil Collins\\\";\n" +
                "$y0 isa address;\n" +
                "$y0 has street \\\"Collins Ave\\\";\n" +
                "$y0 has number 1;\n" +
                "($x0, $y0) isa resides;";

        assertParseEquals(template, json, expected);
    }

    @Test
    public void doubleDotTest(){
        String json  = "{\n" +
                "\t\"person\" : {\n" +
                "\t\t\"name\" : {\n" +
                "\t\t\t\"firstName\" : \"Phil\"\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";

        String template = "$x isa person has name <person.name.firstName>\n";

        String expected = "$x0 isa person has name \\\"Phil\\\"\n";

        assertParseEquals(template, json, expected);
    }

    @Test
    public void comboVarDotTest(){
       String json = "{\n" +
               "\t\"person\" : {\n" +
               "\t\t\"name\" : \"Phil Collins\"\n" +
               "\t}\n" +
               "}";

        String template = "$<person.name> isa person";
        String expected = "$Phil-Collins isa person";

        assertParseEquals(template, json, expected);
    }

    @Test(expected = RuntimeException.class)
    public void wrongDataTest(){
       String json = "{\n" +
               "\t\"person\" : {\n" +
               "\t\t\"name\" : \"Phil Collins\"\n" +
               "\t}\n" +
               "}";

        String template = "$<person.namefhwablfewqhbfli> isa person";
        String expected = "$Phil-Collins isa person";

        assertParseEquals(template, json, expected);
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

        String json = "{ \"firstName\" : \"Phil\" }";
        String expected = "    insert $person0 has name \\\"Phil\\\";";

        assertParseEquals(template, json, expected);

        json = "{}";
        expected = "    insert $person0;";

        assertParseEquals(template, json, expected);
    }

    private void assertParseEquals(String template, String json, String expected){
        String result = parser.parseTemplate(template, Json.read(json));
        System.out.println(result);
        assertEquals(expected, result);
    }
}
