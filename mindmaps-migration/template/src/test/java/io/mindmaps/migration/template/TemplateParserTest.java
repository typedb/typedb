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

public class TemplateParserTest {

    private static TemplateParser parser;

    @BeforeClass
    public static void setup(){
        parser = new TemplateParser();
    }

    @Test
    public void visitFillerTest(){
        String template = "insert $x isa person has name %name ";

        String json = "{\"name\" : \"Phil Collins\"}";
        Map<String, Object> data = Json.read(json).asMap();

        parser.parseTemplate(template, data);
    }


}
