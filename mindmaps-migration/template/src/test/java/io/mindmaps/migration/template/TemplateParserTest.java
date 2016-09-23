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

public class TemplateParserTest {

    private static TemplateParser parser;

    @BeforeClass
    public static void setup(){
        parser = new TemplateParser();
    }

    @Test
    public void oneValueOneLineTest(){
        String template = "insert $x isa person has name %name    ";
        String expected = "insert $x0 isa person has name \\\"Phil Collins\\\"    ";

        String json = "{\"name\" : \"Phil Collins\"}";
        assertParseEquals(template, json, expected);
    }

    @Test
    public void multiValueOneLineTest(){
        String template = "insert $x isa person has name %name , has feet %numFeet";
        String expected = "insert $x0 isa person has name \\\"Phil Collins\\\" , has feet 3";

        String json = "{\"name\" : \"Phil Collins\", \"numFeet\":3}";
        assertParseEquals(template, json, expected);
    }

    @Test(expected = AssertionError.class)
    public void dataMissingTest() {
        String template = "insert $x isa person has name %name , has feet %numFeet ";
        String expected = "insert $x0 isa person has name \\\"Phil Collins\\\", has feet 3 ";

        String json = "{\"name\" : \"Phil Collins\", \"feet\":3}";
        assertParseEquals(template, json, expected);
    }

    @Test
    public void quotingWhenReplacementInVariableTest(){
       String template = "" +
               "insert \n" +
               "( for %address in %addresses){\n" +
               "   $%address has address %address;\n" +
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
        String template = "(for %whale in %whales){" +
                "\t\t\t$x isa whale has name ,%whale;\n}";

        String json = "{\"whales\": [" +
                "\"shamu\"," +
                "\"dory\"" +
                "]}";

        String expected =
                "\t\t\t$x0 isa whale has name ,\\\"shamu\\\";\n" +
                        "\t\t\t$x1 isa whale has name ,\\\"dory\\\";\n";

        assertParseEquals(template, json, expected);
    }

    @Test
    public void multipleDataTypesTest(){
        String template = "first is a %string , second a %long , third a %double , fourth a %bool";
        String expected = "first is a \\\"string\\\" , second a 40 , third a 0.001 , fourth a false";

        String json = "{" +
                "\"string\" : \"string\", " +
                "\"long\" : 40, " +
                "\"double\" : 0.001, " +
                "\"bool\": false}";

        assertParseEquals(template, json, expected);
    }

    @Test
    public void forLoopOverArrayTest(){
        String template = "( for %whale in %whales ) {" +
                "\t\t\t$x isa whale has name %whale ;\n}";

        String json = "{\"whales\": [" +
                "\"shamu\"," +
                "\"dory\"" +
                "]}";

        String expected =
                "\t\t\t$x0 isa whale has name \\\"shamu\\\" ;\n" +
                "\t\t\t$x1 isa whale has name \\\"dory\\\" ;\n";

        assertParseEquals(template, json, expected);
    }

    @Test
    public void forLoopOverObjectsTest(){
        String template = "insert\n" +
                "    $x isa person;\n" +
                "    ( for %addr in %addresses ) {\n" +
                "        $y isa address;\n" +
                "        $y has street %street ;\n" +
                "        $y has number %houseNumber ;\n" +
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
                "( for %person in %people ) {\n" +
                "insert $x isa person has name %name ;\n" +
                "    ( for %address in %addresses ) {\n" +
                "    insert $y isa address ;\n" +
                "        $y has street %street ;\n" +
                "        $y has number %number ;\n" +
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
                "insert $x0 isa person has name \\\"Elmo\\\" ;\n" +
                "    insert $y0 isa address ;\n" +
                "        $y0 has street \\\"North Pole\\\" ;\n" +
                "        $y0 has number 100 ;\n" +
                "        ($x0, $y0) isa resides;\n" +
                "    insert $y1 isa address ;\n" +
                "        $y1 has street \\\"South Pole\\\" ;\n" +
                "        $y1 has number -100 ;\n" +
                "        ($x0, $y1) isa resides;\n" +
                "insert $x1 isa person has name \\\"Flounder\\\" ;\n" +
                "    insert $y2 isa address ;\n" +
                "        $y2 has street \\\"Under the sea\\\" ;\n" +
                "        $y2 has number 22 ;\n" +
                "        ($x1, $y2) isa resides;\n";

        assertParseEquals(template, json, expected);
    }

    private void assertParseEquals(String template, String json, String expected){
        Value result = parser.parseTemplate(template, Json.read(json));
        System.out.println(result.asString());
        assertEquals(expected, result.asString());
    }
}
