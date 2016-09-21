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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;

public class TemplateParserTest {

    private static TemplateParser parser;

    @BeforeClass
    public static void setup(){
        parser = new TemplateParser();
    }

    @Test
    public void oneValueOneLineTest(){
        String template = "insert $x isa person has name %name ";
        String expected = "insert $x isa person has name \"Phil Collins\" ";

        String json = "{\"name\" : \"Phil Collins\"}";
        assertParseEquals(template, json, expected);
    }

    @Test
    public void multiValueOneLineTest(){
        String template = "insert $x isa person has name %name , has feet %numFeet";
        String expected = "insert $x isa person has name \"Phil Collins\" , has feet 3";

        String json = "{\"name\" : \"Phil Collins\", \"numFeet\":3}";
        assertParseEquals(template, json, expected);
    }

    @Test(expected = AssertionError.class)
    public void dataMissingTest() {
        String template = "insert $x isa person has name %name , has feet %numFeet";
        String expected = "insert $x isa person has name \"Phil Collins\", has feet 3 ";

        String json = "{\"name\" : \"Phil Collins\", \"feet\":3}";
        assertParseEquals(template, json, expected);
    }

    @Test
    public void noSpacesBetweenTokensTest(){
        assertTrue(false);
    }

    @Test
    public void multipleDataTypesTest(){
        String template = "first is a %string , second a %long , third a %double , fourth a %bool";
        String expected = "first is a \"string\" , second a 40 , third a 0.001 , fourth a false";

        String json = "{" +
                "\"string\" : \"string\", " +
                "\"long\" : 40, " +
                "\"double\" : 0.001, " +
                "\"bool\": false}";

        assertParseEquals(template, json, expected);
    }

    private void assertParseEquals(String template, String json, String expected){
        Map<String, Object> data = Json.read(json).asMap();
        String result = parser.parseTemplate(template, data);
        assertEquals(expected, result);
    }
}
