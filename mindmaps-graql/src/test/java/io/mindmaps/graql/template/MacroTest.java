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
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class MacroTest {

    private static TemplateParser parser;

    @BeforeClass
    public static void setup(){
        parser = TemplateParser.create();
    }

    @Test
    public void noescpTest(){
        String template = "this is a \"@noescp{ <value> } inside a string\"";
        String expected = "this is a \"whale inside a string\"";

        Map<String, Object> data = Collections.singletonMap("value", "whale");

        assertParseEquals(template, data, expected);
    }

    private void assertParseEquals(String template, Map<String, Object> data, String expected){
        String result = parser.parseTemplate(template, data);
        System.out.println(result);
        assertEquals(expected, result);
    }
}
