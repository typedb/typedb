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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class MacroTest {

    private static TemplateParser parser;

    @BeforeClass
    public static void setup(){
        parser = TemplateParser.create();
    }

    @Test
    public void noescpOneVarTest(){
        String template = "this is a @noescp(value)";
        String expected = "this is a whale";

        Map<String, Object> data = Collections.singletonMap("value", "whale");

        assertParseEquals(template, data, expected);
    }

    @Test
    public void noescpMultiVarTest(){
        String template = "My first name is @noescp(firstname) and my last name is @noescp(lastname)";
        String expected = "My first name is Phil and my last name is Collins";

        Map<String, Object> data = new HashMap<>();
        data.put("firstname", "Phil");
        data.put("lastname", "Collins");

        assertParseEquals(template, data, expected);
    }

    @Test
    public void intMacroTest(){
        String template = "this is an int @int(value)";
        String expected = "this is an int 4";

        assertParseEquals(template, Collections.singletonMap("value", "4"), expected);
        assertParseEquals(template, Collections.singletonMap("value", 4), expected);
    }

    @Test
    public void doubleMacroTest(){
        String template = "this is a double @double(value)";
        String expected = "this is a double 4.0";

        assertParseEquals(template, Collections.singletonMap("value", "4.0"), expected);
        assertParseEquals(template, Collections.singletonMap("value", 4.0), expected);
    }

    @Test
    public void equalsMacroTest(){
        String template = "@equals(this that)";
        String expected = "true";

        Map<String, Object> data = new HashMap<>();
        data.put("this", "50");
        data.put("that", "50");

        assertParseEquals(template, data, expected);

        template = "@equals(this notThat)";
        expected = "false";

        data = new HashMap<>();
        data.put("this", "50");
        data.put("notThat", "500");

        assertParseEquals(template, data, expected);

        template = "@equals(this notThat)";
        expected = "false";

        data = new HashMap<>();
        data.put("this", "50");
        data.put("notThat", 50);

        assertParseEquals(template, data, expected);

        template = "@equals(this that those)";
        expected = "true";

        data = new HashMap<>();
        data.put("this", 50);
        data.put("that", 50);
        data.put("those", 50);

        assertParseEquals(template, data, expected);

        template = "@equals(this that notThat)";
        expected = "false";

        data = new HashMap<>();
        data.put("this", 50);
        data.put("that", 50);
        data.put("notThat", 50.0);

        assertParseEquals(template, data, expected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongNumberOfArgumentsTest(){
        String template = "@noescp(value otherValue)";
        parser.parseTemplate(template, new HashMap<>());
    }

    @Test
    public void macroInArgumentTest(){

        String template = "if (@equals (this that)) do { equals } else { not }";
        String expected = " equals";
        Map<String, Object> data = new HashMap<>();
        data.put("this", "50");
        data.put("that", "50");

        assertParseEquals(template, data, expected);

        expected = " not";
        data = new HashMap<>();
        data.put("this", "50");
        data.put("that", "500");

        assertParseEquals(template, data, expected);
    }

    private void assertParseEquals(String template, Map<String, Object> data, String expected){
        String result = parser.parseTemplate(template, data);
        System.out.println(result);
        assertEquals(expected, result);
    }
}
