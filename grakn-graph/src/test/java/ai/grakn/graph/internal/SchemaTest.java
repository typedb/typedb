/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graph.internal;

import ai.grakn.util.Schema;
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
        Schema.EdgeLabel sub = Schema.EdgeLabel.valueOf("SUB");
        Schema.EdgeLabel relates = Schema.EdgeLabel.valueOf("RELATES");
        Schema.EdgeLabel has_scope = Schema.EdgeLabel.valueOf("HAS_SCOPE");
        Schema.EdgeLabel casting = Schema.EdgeLabel.valueOf("CASTING");

        Schema.MetaSchema role_type = Schema.MetaSchema.valueOf("ROLE");

        // Basic Edges
        assertEquals(Schema.EdgeLabel.ISA, isa);
        assertEquals(Schema.EdgeLabel.SUB, sub);
        assertEquals(Schema.EdgeLabel.RELATES, relates);
        assertEquals(Schema.EdgeLabel.HAS_SCOPE, has_scope);
        // Other
        assertEquals(Schema.EdgeLabel.CASTING, casting);

        //Internal Vertex
        assertEquals(Schema.MetaSchema.ROLE, role_type);

    }
}
