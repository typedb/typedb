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

package io.mindmaps.graph.internal;

import io.mindmaps.util.Schema;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.junit.Assert.assertEquals;

public class SchemaTest {
    @Test(expected = InvocationTargetException.class)
    public void testConstructor() throws Exception { //Checks that you cannot initialise it.
        Constructor<Schema> c = Schema.class.getDeclaredConstructor();
        c.setAccessible(true);
        Schema u = c.newInstance();
    }

    @Test
    public void enumParingAndValueTest(){
        Schema.EdgeLabel isa = Schema.EdgeLabel.valueOf("ISA");
        Schema.EdgeLabel ako = Schema.EdgeLabel.valueOf("AKO");
        Schema.EdgeLabel has_role = Schema.EdgeLabel.valueOf("HAS_ROLE");
        Schema.EdgeLabel has_scope = Schema.EdgeLabel.valueOf("HAS_SCOPE");
        Schema.EdgeLabel casting = Schema.EdgeLabel.valueOf("CASTING");

        Schema.MetaType role_type = Schema.MetaType.valueOf("ROLE_TYPE");

        // Basic Edges
        assertEquals(Schema.EdgeLabel.ISA, isa);
        assertEquals(Schema.EdgeLabel.AKO, ako);
        assertEquals(Schema.EdgeLabel.HAS_ROLE, has_role);
        assertEquals(Schema.EdgeLabel.HAS_SCOPE, has_scope);
        // Other
        assertEquals(Schema.EdgeLabel.CASTING, casting);

        //Internal Vertex
        assertEquals(Schema.MetaType.ROLE_TYPE, role_type);

    }
}
