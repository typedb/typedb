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
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;

public class MacroTest {

    private static TemplateParser parser;

    @BeforeClass
    public static void setup(){
        parser = TemplateParser.create();
    }

    @Test
    public void noescpOneVarTest(){
        String template = "this is a @noescp{<value>}";
        String expected = "this is a whale";

        Map<String, Object> data = Collections.singletonMap("value", "whale");

        assertParseEquals(template, data, expected);
    }

    @Test
    public void noescpMultiVarTest(){
        String template = "My first name is @noescp{<firstname> and my last name is <lastname>} ";
        String expected = "My first name is Phil and my last name is Collins ";

        Map<String, Object> data = new HashMap<>();
        data.put("firstname", "Phil");
        data.put("lastname", "Collins");

        assertParseEquals(template, data, expected);
    }

    @Ignore
    @Test
    public void variablesInsideMacroBlockTest(){
        assertTrue(false);
    }

    private void assertParseEquals(String template, Map<String, Object> data, String expected){
        String result = parser.parseTemplate(template, data);
        System.out.println(result);
        assertEquals(expected, result);
    }
}
